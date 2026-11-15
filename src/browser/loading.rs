//! Fetching without freezing the screen: opening a file or saving a download
//! runs as a job on a worker thread while the viewer pane draws a progress bar
//! from the shared [`Progress`], and the UI thread applies the [`Done`] it hands
//! back. Dropping a [`Loading`] cancels it — a cloud download stops once the
//! ranges in flight land; a local read or a parse runs to the end and is thrown
//! away.

use crate::browser::download::human_size;
use crate::browser::{FileBrowser, Progress};
use crate::text_viewer::TextLoad;
use crate::theme::Theme;
use polars::prelude::DataFrame;
use ratatui::layout::{Alignment, Constraint, Layout, Rect};
use ratatui::style::Style;
use ratatui::widgets::{Block, Borders, Gauge, Paragraph};
use ratatui::Frame;
use std::sync::atomic::Ordering;
use std::sync::mpsc::{self, TryRecvError};
use std::sync::Arc;

/// What a finished job hands the UI thread to apply.
pub enum Done {
    /// A frame for the table viewer, and its title.
    Table(DataFrame, String),
    /// Lines for the text viewer, and its title.
    Text(TextLoad, String),
    /// Nothing to show but a status line: a download's outcome, or why a file
    /// would not open.
    Status(String),
}

pub struct Loading {
    /// Heads the bar: the file being opened or saved.
    pub title: String,
    /// A save outlives the app, so quitting would throw it away: its first `q`
    /// only arms [`Self::quit_armed`]. An open dies with the app anyway.
    pub saves: bool,
    /// A save's first `q` landed and the warning is up; the next one quits.
    pub quit_armed: bool,
    /// The bar's label once every byte is in — the parse, or the write to disk.
    finishing: &'static str,
    progress: Arc<Progress>,
    rx: mpsc::Receiver<Done>,
}

impl Loading {
    pub fn start<F>(
        title: String,
        finishing: &'static str,
        backend: Arc<dyn FileBrowser>,
        job: F,
    ) -> Self
    where
        F: FnOnce(&dyn FileBrowser, &Progress) -> Done + Send + 'static,
    {
        let progress = Arc::new(Progress::default());
        let (tx, rx) = mpsc::channel();
        let worker_progress = Arc::clone(&progress);
        std::thread::spawn(move || {
            // Nobody is listening once the job was cancelled.
            let _ = tx.send(job(backend.as_ref(), &worker_progress));
        });
        Self {
            title,
            saves: false,
            quit_armed: false,
            finishing,
            progress,
            rx,
        }
    }

    /// The finished job, or `None` while it is still running. Errs only when
    /// the worker died without answering — it panicked, and ratatui's panic
    /// hook has already restored the terminal, so the caller has to exit.
    pub fn poll(&self) -> Result<Option<Done>, TryRecvError> {
        match self.rx.try_recv() {
            Ok(loaded) => Ok(Some(loaded)),
            Err(TryRecvError::Empty) => Ok(None),
            Err(e) => Err(e),
        }
    }
}

impl Drop for Loading {
    fn drop(&mut self) {
        self.progress.cancel.store(true, Ordering::Relaxed);
    }
}

/// The bar's fill and label. `total` stays 0 for a local file (nothing to
/// download) and until a cloud download learns the object's size.
fn bar(done: usize, total: usize, finishing: &str) -> (f64, String) {
    if total == 0 {
        (0.0, "Loading…".into())
    } else if done < total {
        (
            done as f64 / total as f64,
            format!(
                "Downloading {} / {}",
                human_size(done as u64),
                human_size(total as u64)
            ),
        )
    } else {
        (1.0, finishing.into())
    }
}

pub fn render(frame: &mut Frame, loading: &Loading, area: Rect, theme: &Theme) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.border_idle))
        .title(format!(" {} ", loading.title));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let [_, row, _, warning, _] = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Length(1),
        Constraint::Fill(1),
    ])
    .areas(inner);
    if loading.quit_armed {
        let text = Paragraph::new("Press q again to quit — the download will be lost")
            .alignment(Alignment::Center)
            .style(Style::default().fg(theme.error));
        frame.render_widget(text, warning);
    }
    let [_, row, _] = Layout::horizontal([
        Constraint::Percentage(15),
        Constraint::Percentage(70),
        Constraint::Percentage(15),
    ])
    .areas(row);

    let (ratio, label) = bar(
        loading.progress.done.load(Ordering::Relaxed),
        loading.progress.total.load(Ordering::Relaxed),
        loading.finishing,
    );
    let gauge = Gauge::default()
        .ratio(ratio)
        .label(label)
        .use_unicode(true)
        .gauge_style(Style::default().fg(theme.accent).bg(theme.bg_alt));
    frame.render_widget(gauge, row);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bar_reports_each_phase() {
        assert_eq!(bar(0, 0, "Saving…"), (0.0, "Loading…".to_string()));
        assert_eq!(
            bar(512 * 1024, 2 * 1024 * 1024, "Saving…"),
            (0.25, "Downloading 512.0 KiB / 2.0 MiB".to_string())
        );
        assert_eq!(bar(10, 10, "Saving…"), (1.0, "Saving…".to_string()));
    }
}
