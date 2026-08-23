//! Fuzzy find over the current browser listing.
//!
//! `/` opens a prompt that owns the keyboard and re-ranks the listing on every
//! keystroke. The matching is written out here rather than pulled from a crate:
//! one directory listing is small enough that scoring quality matters more than
//! algorithmic sophistication, and owning it means the ranking can be tuned and
//! unit-tested with no dependency.
//!
//! [`rank`] is pure — it is the whole matcher, and the only part worth testing
//! in isolation. [`FindPrompt`] and [`prompt_line`] are the TUI half, kept in
//! the same module the way `download.rs` keeps its prompt beside `resolve_dest`.
//!
//! Left out on purpose: space-separated multi-term queries. A space is a literal
//! character here, so it matches names that contain one.

use crate::browser::Entry;
use crate::theme::Theme;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};

/// One entry that survived the query, with the score it was ranked by and the
/// char positions the renderer highlights.
///
/// Positions are ascending, unique, and counted in `char`s rather than bytes —
/// the renderer slices by chars, so a multi-byte name highlights the character
/// that actually matched.
#[derive(Debug, Clone, PartialEq)]
pub struct Match {
    /// Index into the listing this was ranked against.
    pub index: usize,
    pub score: i32,
    pub positions: Vec<usize>,
}

/// The text typed after `/`. `Some(..)` on [`crate::browser::app::BrowserApp`]
/// means the prompt owns the keyboard, the same gating the download prompt and
/// the theme picker use.
pub struct FindPrompt {
    pub query: String,
}

impl FindPrompt {
    pub fn open() -> Self {
        Self {
            query: String::new(),
        }
    }
}

// Scoring weights. The absolute values do not matter, only their ratios: a match
// at a word boundary or one continuing a run is worth roughly half a match
// again, and a gap costs less than a match is worth, so a scattered match still
// beats no match at all.
const MATCH: i32 = 16;
const BONUS_BOUNDARY: i32 = 8;
const BONUS_CONSECUTIVE: i32 = 8;
const PENALTY_GAP_START: i32 = -3;
const PENALTY_GAP_EXTEND: i32 = -1;

/// Rank `entries` against `query`, best match first.
///
/// An empty query is the identity: every entry, in listing order, unranked.
pub fn rank(entries: &[Entry], query: &str) -> Vec<Match> {
    if query.is_empty() {
        return (0..entries.len())
            .map(|index| Match {
                index,
                score: 0,
                positions: Vec::new(),
            })
            .collect();
    }

    // Smart case, the way fzf does it: an uppercase char anywhere in the query
    // makes the whole match case-sensitive, so a lowercase query stays forgiving
    // and a deliberate capital narrows.
    let case_sensitive = query.chars().any(char::is_uppercase);
    let needle: Vec<char> = query.chars().map(|c| fold(c, case_sensitive)).collect();

    let mut matches: Vec<Match> = entries
        .iter()
        .enumerate()
        .filter_map(|(index, entry)| {
            let haystack: Vec<char> = entry.name.chars().collect();
            best_match(&haystack, &needle, case_sensitive).map(|(score, positions)| Match {
                index,
                score,
                positions,
            })
        })
        .collect();

    // Shorter names win equal scores — the same query matching less text is the
    // tighter hit. `sort_by` is stable, so a full tie keeps the backend's order,
    // which lists directories first.
    matches.sort_by(|a, b| {
        b.score.cmp(&a.score).then_with(|| {
            entries[a.index]
                .name
                .len()
                .cmp(&entries[b.index].name.len())
        })
    });
    matches
}

/// Case-fold one char, preserving a one-to-one char mapping.
///
/// `char::to_lowercase` yields an iterator because some chars expand — `ß`
/// becomes `ss`. Taking the first keeps every position aligned with the string
/// the renderer highlights, which an expanding fold would silently desynchronise.
fn fold(c: char, case_sensitive: bool) -> char {
    if case_sensitive {
        c
    } else {
        c.to_lowercase().next().unwrap_or(c)
    }
}

/// The highest-scoring greedy match, tried from every position the first needle
/// char occurs at.
///
/// A single greedy pass locks onto the earliest occurrence of each char and
/// never sees a tighter run further along: `ab` against `a_xb_ab` would settle
/// for `a(0), b(3)` and miss the consecutive `a(5), b(6)`. Re-running from each
/// anchor finds it. A listing is small and names are short, so the extra passes
/// cost nothing measurable.
fn best_match(
    haystack: &[char],
    needle: &[char],
    case_sensitive: bool,
) -> Option<(i32, Vec<usize>)> {
    let first = *needle.first()?;
    let mut best: Option<(i32, Vec<usize>)> = None;
    for anchor in 0..haystack.len() {
        if fold(haystack[anchor], case_sensitive) != first {
            continue;
        }
        if let Some(candidate) = greedy_from(haystack, needle, anchor, case_sensitive) {
            if best
                .as_ref()
                .map_or(true, |(score, _)| candidate.0 > *score)
            {
                best = Some(candidate);
            }
        }
    }
    best
}

/// Walk the needle through the haystack from `anchor`, taking the first
/// occurrence of each remaining char. `None` if the needle does not fit.
fn greedy_from(
    haystack: &[char],
    needle: &[char],
    anchor: usize,
    case_sensitive: bool,
) -> Option<(i32, Vec<usize>)> {
    let mut positions = Vec::with_capacity(needle.len());
    let mut score = 0;
    let mut previous = anchor;

    for (n, &want) in needle.iter().enumerate() {
        let at = if n == 0 {
            anchor // already known to match — that is what anchored the run
        } else {
            (previous + 1..haystack.len()).find(|&i| fold(haystack[i], case_sensitive) == want)?
        };

        score += MATCH;
        if is_boundary(haystack, at) {
            score += BONUS_BOUNDARY;
        }
        if n > 0 {
            match at - previous - 1 {
                0 => score += BONUS_CONSECUTIVE,
                gap => score += PENALTY_GAP_START + PENALTY_GAP_EXTEND * (gap as i32 - 1),
            }
        }

        positions.push(at);
        previous = at;
    }

    Some((score, positions))
}

/// Whether `i` reads as the start of a word: the first char, one following a
/// separator, or the upper half of a camelCase edge.
fn is_boundary(haystack: &[char], i: usize) -> bool {
    if i == 0 {
        return true;
    }
    let previous = haystack[i - 1];
    matches!(previous, '_' | '-' | '.' | '/' | ' ')
        || (!previous.is_uppercase() && haystack[i].is_uppercase())
}

/// Bottom-bar line while the prompt is open.
///
/// The prompt takes over the shortcut bar, so it carries its own key hints the
/// way the download prompt does — with the bar gone, nothing else on screen
/// would say how to get out.
pub fn prompt_line(
    prompt: &FindPrompt,
    shown: usize,
    total: usize,
    theme: &Theme,
) -> Line<'static> {
    // A query matching nothing is worth flagging in the bar: the list beside it
    // is empty, and the reason is the text that was just typed.
    let (text, bg) = if shown == 0 && !prompt.query.is_empty() {
        (
            format!(" / {}_ — no matches   Esc cancel ", prompt.query),
            theme.warn,
        )
    } else {
        (
            format!(
                " / {}_   {}/{}   Enter open · ↑↓ move · Esc cancel ",
                prompt.query, shown, total
            ),
            theme.info,
        )
    };
    Line::from(Span::styled(
        text,
        Style::default()
            .bg(bg)
            .fg(theme.bg)
            .add_modifier(Modifier::BOLD),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::browser::classify;

    fn entries(names: &[&str]) -> Vec<Entry> {
        names
            .iter()
            .map(|name| Entry {
                kind: classify(name),
                name: name.to_string(),
                path: format!("/test/{}", name),
            })
            .collect()
    }

    /// Names in ranked order, which is what the browser pane actually shows.
    fn ranked(names: &[&str], query: &str) -> Vec<String> {
        let entries = entries(names);
        rank(&entries, query)
            .iter()
            .map(|m| entries[m.index].name.clone())
            .collect()
    }

    #[test]
    fn empty_query_is_the_identity() {
        let names = ["b.csv", "a.csv", "sub"];
        let matched = rank(&entries(&names), "");
        assert_eq!(
            matched.iter().map(|m| m.index).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "an empty query must not reorder the listing"
        );
        assert!(matched.iter().all(|m| m.positions.is_empty()));
    }

    #[test]
    fn a_query_that_matches_nothing_ranks_nothing() {
        assert!(ranked(&["orders.csv", "sales.csv"], "zzz").is_empty());
    }

    #[test]
    fn matching_is_a_subsequence_not_a_substring() {
        assert_eq!(ranked(&["orders.csv"], "ors"), vec!["orders.csv"]);
    }

    #[test]
    fn matching_ignores_case_for_a_lowercase_query() {
        assert_eq!(ranked(&["Orders.CSV"], "orders"), vec!["Orders.CSV"]);
    }

    #[test]
    fn an_uppercase_char_makes_the_whole_query_case_sensitive() {
        // Smart case: the capital is a deliberate narrowing, so the lowercase
        // name drops out entirely.
        assert_eq!(ranked(&["Sales.csv", "sales.csv"], "S"), vec!["Sales.csv"]);
        assert_eq!(ranked(&["Sales.csv", "sales.csv"], "s").len(), 2);
    }

    #[test]
    fn the_tightest_run_wins_over_the_earliest_one() {
        // The case a single greedy pass gets wrong: it would lock onto a(0) and
        // settle for b(3) rather than backing up to the consecutive a(5), b(6).
        let matched = rank(&entries(&["a_xb_ab"]), "ab");
        assert_eq!(matched[0].positions, vec![5, 6]);
    }

    #[test]
    fn a_match_at_a_word_boundary_outranks_one_mid_word() {
        // 'c' after the '_' in sales_count beats the 'c' buried in discount.
        assert_eq!(
            ranked(&["discount.csv", "sales_count.csv"], "sc"),
            vec!["sales_count.csv", "discount.csv"]
        );
    }

    #[test]
    fn a_consecutive_run_outranks_a_scattered_match() {
        assert_eq!(
            ranked(&["wholesale.csv", "sales.csv"], "sal"),
            vec!["sales.csv", "wholesale.csv"]
        );
    }

    #[test]
    fn a_camel_case_edge_counts_as_a_boundary() {
        assert!(is_boundary(&"salesCount".chars().collect::<Vec<_>>(), 5));
    }

    #[test]
    fn a_shorter_name_wins_an_equal_score() {
        // Both match "sales" consecutively from the start, so only length separates
        // them.
        assert_eq!(
            ranked(&["sales_2024_q1.csv", "sales.csv"], "sales"),
            vec!["sales.csv", "sales_2024_q1.csv"]
        );
    }

    #[test]
    fn a_full_tie_keeps_the_listing_order() {
        // The backend lists directories first; a stable sort is what preserves that
        // when the query cannot tell two entries apart.
        assert_eq!(ranked(&["ab.csv", "ac.csv"], "a"), vec!["ab.csv", "ac.csv"]);
    }

    #[test]
    fn positions_are_char_indices_not_byte_offsets() {
        // 'é' is two bytes: a byte-indexed matcher would report l at 3 and the
        // renderer would highlight the wrong char.
        let matched = rank(&entries(&["héllo.csv"]), "él");
        assert_eq!(matched[0].positions, vec![1, 2]);
    }

    #[test]
    fn positions_are_ascending_and_unique() {
        let matched = rank(&entries(&["orders_2024.csv"]), "ordcsv");
        let positions = &matched[0].positions;
        assert!(
            positions.windows(2).all(|w| w[0] < w[1]),
            "positions must be strictly ascending: {:?}",
            positions
        );
    }

    #[test]
    fn every_query_char_gets_a_position() {
        let matched = rank(&entries(&["sales_count.csv"]), "sc");
        assert_eq!(matched[0].positions.len(), 2);
    }

    // ── prompt line ───────────────────────────────────────────────────────────

    fn prompt_text(prompt: &FindPrompt, shown: usize, total: usize) -> String {
        prompt_line(prompt, shown, total, crate::theme::default_theme())
            .spans
            .iter()
            .map(|s| s.content.as_ref())
            .collect()
    }

    #[test]
    fn prompt_line_shows_the_query_and_the_match_count() {
        let prompt = FindPrompt {
            query: "sal".to_string(),
        };
        let text = prompt_text(&prompt, 3, 47);
        assert!(text.contains("sal"), "missing query: {}", text);
        assert!(text.contains("3/47"), "missing count: {}", text);
        assert!(text.contains("Esc cancel"), "missing exit hint: {}", text);
    }

    #[test]
    fn prompt_line_flags_a_query_that_matches_nothing() {
        let prompt = FindPrompt {
            query: "zzz".to_string(),
        };
        let text = prompt_text(&prompt, 0, 47);
        assert!(text.contains("no matches"), "unexpected line: {}", text);
    }

    #[test]
    fn prompt_line_does_not_cry_no_matches_before_anything_is_typed() {
        let text = prompt_text(&FindPrompt::open(), 0, 0);
        assert!(!text.contains("no matches"), "unexpected line: {}", text);
    }
}
