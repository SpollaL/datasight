use polars::prelude::*;
use ratatui::widgets::TableState;
use std::collections::HashMap;

pub struct ColumnProfile {
    pub name: String,
    pub dtype: String,
    pub count: usize,
    pub null_count: usize,
    pub unique: usize,
    pub min: String,
    pub max: String,
    pub mean: Option<f64>,
    pub median: Option<f64>,
}

const DEFAULT_COLUMN_WIDTH: u16 = 15;

pub struct Config {
    pub file_path: String,
}

impl Config {
    pub fn new(mut args: impl Iterator<Item = String>) -> Result<Config, &'static str> {
        args.next();
        let file_path = match args.next() {
            Some(arg) => arg,
            None => return Err("Please provide a valid CSV file path"),
        };
        Ok(Config { file_path })
    }
}

#[derive(Debug)]
pub enum Mode {
    Search,
    Normal,
    Filter,
    PlotPickX,
    Plot,
    ColumnsView,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlotType {
    Line,
    Bar,
}

#[derive(Debug)]
pub enum SortDirection {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Sum,
    Mean,
    Count,
    Min,
    Max,
}

#[derive(Default)]
pub struct ColumnStats {
    pub count: usize,
    pub min: String,
    pub max: String,
    pub mean: Option<f64>,
    pub median: Option<f64>,
}

pub struct App {
    pub df: DataFrame,        // original data
    pub view: DataFrame,      // current filtered/sorted result
    pub headers: Vec<String>, // column names for display
    pub state: TableState,
    pub should_quit: bool,
    pub file_path: String,
    pub column_widths: Vec<u16>,
    pub mode: Mode,
    pub search_query: String,
    pub search_results: Vec<usize>,
    pub search_cursor: usize,
    pub filters: Vec<(usize, String)>,
    pub filter_input: String,
    pub sort_column: Option<usize>,
    pub sort_direction: SortDirection,
    pub show_stats: bool,
    pub show_help: bool,
    pub groupby_keys: Vec<usize>,
    pub groupby_aggs: HashMap<usize, AggFunc>,
    pub groupby_active: bool,
    pub saved_headers: Vec<String>,
    pub saved_column_widths: Vec<u16>,
    pub plot_y_col: Option<usize>,
    pub plot_x_col: Option<usize>,
    pub plot_type: PlotType,
    pub columns_profile: Vec<ColumnProfile>,
    pub columns_view_state: TableState,
    pub view_offset: usize,
}

impl App {
    pub fn new(df: DataFrame, file_path: String) -> App {
        let headers: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let column_count = headers.len();
        let view = df.clone();
        let mut app = App {
            df: df,
            view: view,
            headers: headers,
            state: TableState::default(),
            should_quit: false,
            file_path,
            column_widths: vec![DEFAULT_COLUMN_WIDTH; column_count],
            mode: Mode::Normal,
            search_query: String::new(),
            search_results: Vec::new(),
            search_cursor: 0,
            filter_input: String::new(),
            filters: Vec::new(),
            sort_column: None,
            sort_direction: SortDirection::Ascending,
            show_stats: false,
            show_help: false,
            groupby_keys: Vec::new(),
            groupby_aggs: HashMap::new(),
            groupby_active: false,
            saved_headers: Vec::new(),
            saved_column_widths: Vec::new(),
            plot_y_col: None,
            plot_x_col: None,
            plot_type: PlotType::Line,
            columns_profile: Vec::new(),
            columns_view_state: TableState::default(),
            view_offset: 0,
        };
        if !app.df.is_empty() {
            app.state.select(Some(0));
            app.state.select_column(Some(0));
        }
        app
    }

    pub fn update_search(&mut self) {
        let current_column = self.state.selected_column().unwrap_or(0);
        if self.headers.is_empty() || current_column >= self.headers.len() || self.view.is_empty() {
            self.search_results.clear();
            return;
        }
        let col_name = &self.headers[current_column];
        let query = self.search_query.to_lowercase();
        let Some(series) = self
            .view
            .column(col_name)
            .ok()
            .and_then(|c| c.as_series())
            .and_then(|s| s.cast(&DataType::String).ok())
        else {
            self.search_results.clear();
            return;
        };
        self.search_results = series
            .str()
            .map(|ca| {
                ca.into_iter()
                    .enumerate()
                    .filter(|(_, v)| v.is_some_and(|s| s.to_lowercase().contains(&query)))
                    .map(|(i, _)| i)
                    .collect()
            })
            .unwrap_or_default();
        self.search_cursor = 0;
    }

    pub fn update_filter(&mut self) {
        let mut mask = lit(true);
        for (colidx, query) in &self.filters {
            let col_name = &self.headers[*colidx];
            mask = mask.and(
                col(col_name)
                    .cast(DataType::String)
                    .str()
                    .contains(lit(query.as_str()), false),
            );
        }
        if !self.filter_input.is_empty() {
            let col_name = &self.headers[self.state.selected_column().unwrap_or(0)];
            mask = mask.and(
                col(col_name)
                    .cast(DataType::String)
                    .str()
                    .contains(lit(self.filter_input.as_str()), false),
            )
        }
        let filtered = self
            .df
            .clone()
            .lazy()
            .filter(mask)
            .collect()
            .unwrap_or(self.df.clone());

        self.view_offset = 0;
        self.view = if let Some(sort_col) = self.sort_column {
            let col_name = &self.headers[sort_col];
            let opts = SortMultipleOptions::default()
                .with_order_descending(matches!(self.sort_direction, SortDirection::Descending));
            match filtered.sort([col_name], opts) {
                Ok(sorted) => sorted,
                Err(_) => filtered,
            }
        } else {
            filtered
        };
        if !self.search_query.is_empty() {
            self.update_search();
        }
    }

    pub fn sort_by_column(&mut self) {
        let current_column = self.state.selected_column().unwrap_or(0);
        if self.sort_column == Some(current_column) {
            self.sort_direction = match self.sort_direction {
                SortDirection::Ascending => SortDirection::Descending,
                SortDirection::Descending => SortDirection::Ascending,
            };
        } else {
            self.sort_column = Some(current_column);
            self.sort_direction = SortDirection::Ascending;
        }
        let col_name = &self.headers[current_column];
        let opts = SortMultipleOptions::default()
            .with_order_descending(matches!(self.sort_direction, SortDirection::Descending));
        self.view = match self.view.sort([col_name], opts) {
            Ok(sorted) => sorted,
            Err(_) => self.view.clone(),
        };
        if !self.search_query.is_empty() {
            self.update_search();
        }
    }

    pub fn autofit_selected_column(&mut self) {
        if let Some(col_idx) = self.state.selected_column() {
            let label = self.header_label(col_idx);
            let header_width = label.chars().count() as u16;
            let col_name = self.headers[col_idx].clone();
            let max_data = self
                .view
                .column(&col_name)
                .ok()
                .and_then(|col| {
                    let cast = col.as_series()?.cast(&DataType::String).ok()?;
                    let max = cast
                        .str()
                        .ok()?
                        .into_iter()
                        .filter_map(|v| v)
                        .map(|s: &str| s.chars().count())
                        .max()
                        .map(|n| n as u16);
                    max
                })
                .unwrap_or(0);
            self.column_widths[col_idx] = max_data.max(header_width);
        }
    }

    pub fn autofit_all_columns(&mut self) {
        let cols: Vec<usize> = (0..self.headers.len()).collect();
        for col_idx in cols {
            let label = self.header_label(col_idx);
            let header_width = label.chars().count() as u16;
            let col_name = self.headers[col_idx].clone();
            let max_data = self
                .view
                .column(&col_name)
                .ok()
                .and_then(|col| {
                    let cast = col.as_series()?.cast(&DataType::String).ok()?;
                    cast.str()
                        .ok()?
                        .into_iter()
                        .flatten()
                        .map(|s| s.chars().count())
                        .max()
                        .map(|n| n as u16)
                })
                .unwrap_or(0);
            self.column_widths[col_idx] = max_data.max(header_width);
        }
    }

    pub fn header_label(&self, col_idx: usize) -> String {
        let base = &self.headers[col_idx];
        let label = if self.sort_column == Some(col_idx) {
            let dir = if matches!(self.sort_direction, SortDirection::Descending) {
                "▼"
            } else {
                "▲"
            };
            format!("{} {}", base, dir)
        } else {
            base.clone()
        };
        if self.groupby_keys.contains(&col_idx) {
            format!("{} [K]", label)
        } else if let Some(func) = self.groupby_aggs.get(&col_idx) {
            let sym = match func {
                AggFunc::Sum => "Σ",
                AggFunc::Mean => "μ",
                AggFunc::Count => "#",
                AggFunc::Min => "↓",
                AggFunc::Max => "↑",
            };
            format!("{} [{}]", label, sym)
        } else {
            label
        }
    }

    pub fn compute_stats(&mut self, col: usize) -> ColumnStats {
        if col >= self.headers.len() {
            return ColumnStats::default();
        }
        let col_name = &self.headers[col];
        let Ok(series) = self.view.column(col_name) else {
            return ColumnStats::default();
        };
        let count = series.len();
        let min = series
            .min_reduce()
            .ok()
            .map(|s| s.value().to_string())
            .unwrap_or_default();
        let max = series
            .max_reduce()
            .ok()
            .map(|s| s.value().to_string())
            .unwrap_or_default();
        let (mean, median) = series
            .as_series()
            .map(|s| (s.mean(), s.median()))
            .unwrap_or((None, None));
        ColumnStats { count, min, max, mean, median }
    }

    pub fn toggle_groupby_key(&mut self) {
        let col = self.state.selected_column().unwrap_or(0);
        if let Some(pos) = self.groupby_keys.iter().position(|&k| k == col) {
            self.groupby_keys.remove(pos);
        } else {
            self.groupby_keys.push(col);
            self.groupby_aggs.remove(&col);
        }
    }

    pub fn cycle_groupby_agg(&mut self) {
        let col = self.state.selected_column().unwrap_or(0);
        if self.groupby_keys.contains(&col) {
            return;
        };
        let next = match self.groupby_aggs.get(&col) {
            None => Some(AggFunc::Sum),
            Some(AggFunc::Sum) => Some(AggFunc::Mean),
            Some(AggFunc::Mean) => Some(AggFunc::Count),
            Some(AggFunc::Count) => Some(AggFunc::Min),
            Some(AggFunc::Min) => Some(AggFunc::Max),
            Some(AggFunc::Max) => None,
        };
        match next {
            Some(f) => { self.groupby_aggs.insert(col, f); }
            None => { self.groupby_aggs.remove(&col); }
        };
    }
    pub fn apply_groupby(&mut self) {
        if self.groupby_keys.is_empty() || self.groupby_aggs.is_empty() {
            return;
        }
        let key_exprs: Vec<Expr> = self
            .groupby_keys
            .iter()
            .map(|&i| col(&self.headers[i]))
            .collect();
        let agg_exprs: Vec<Expr> = self
            .groupby_aggs
            .iter()
            .map(|(i, func)| {
                let name = &self.headers[*i];
                match func {
                    AggFunc::Sum => col(name).sum().alias(&format!("{}_sum", name)),
                    AggFunc::Mean => col(name).mean().alias(&format!("{}_mean", name)),
                    AggFunc::Count => col(name).count().alias(&format!("{}_count", name)),
                    AggFunc::Min => col(name).min().alias(&format!("{}_min", name)),
                    AggFunc::Max => col(name).max().alias(&format!("{}_max", name)),
                }
            })
            .collect();
        let first_key = self.headers[self.groupby_keys[0]].clone();
        let result = self
            .view
            .clone()
            .lazy()
            .group_by(key_exprs)
            .agg(agg_exprs)
            .sort([&first_key], SortMultipleOptions::default())
            .collect();
        if let Ok(df) = result {
            self.view_offset = 0;
            self.saved_headers = self.headers.clone();
            self.saved_column_widths = self.column_widths.clone();
            self.headers = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            self.column_widths = vec![DEFAULT_COLUMN_WIDTH; df.width()];
            self.sort_column = None;
            self.search_results = Vec::new();
            self.search_cursor = 0;
            self.view = df;
            self.groupby_active = true;
            self.state.select(Some(0));
            self.state.select_column(Some(0));
        }
    }

    pub fn build_columns_profile(&mut self) {
        self.columns_profile = self
            .view
            .get_columns()
            .iter()
            .map(|col| {
                let name = col.name().to_string();
                let dtype = col.dtype().to_string();
                let count = col.len();
                let null_count = col.null_count();
                let unique = col
                    .as_series()
                    .and_then(|s| s.n_unique().ok())
                    .unwrap_or(0);
                let min = col
                    .min_reduce()
                    .ok()
                    .map(|s| s.value().to_string())
                    .unwrap_or_default();
                let max = col
                    .max_reduce()
                    .ok()
                    .map(|s| s.value().to_string())
                    .unwrap_or_default();
                let mean = col.as_series().and_then(|s| s.mean());
                let median = col.as_series().and_then(|s| s.median());
                ColumnProfile { name, dtype, count, null_count, unique, min, max, mean, median }
            })
            .collect();
        self.columns_view_state.select(Some(0));
    }

    pub fn plot_type_label(&self) -> &str {
        match self.plot_type {
            PlotType::Line => "Line",
            PlotType::Bar => "Bar",
        }
    }

    pub fn clear_groupby(&mut self) {
        self.view_offset = 0;
        self.headers = self.saved_headers.clone();
        self.column_widths = self.saved_column_widths.clone();
        self.groupby_keys = Vec::new();
        self.groupby_aggs = HashMap::new();
        self.groupby_active = false;
        self.sort_column = None;
        self.search_results = Vec::new();
        self.search_cursor = 0;
        self.update_filter();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_app() -> App {
        let df = df! {
            "name" => ["Alice", "Bob", "Charlie"],
            "age" => [30i64, 25, 35],
        }
        .unwrap();
        App::new(df, "test.csv".to_string())
    }

    fn get_str(app: &App, col: &str, row: usize) -> String {
        app.view
            .column(col)
            .unwrap()
            .as_series()
            .unwrap()
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .get(row)
            .unwrap_or("")
            .to_string()
    }

    #[test]
    fn test_update_search_finds_matches() {
        let mut app = make_app();
        app.search_query = "alice".to_string();
        app.update_search();
        assert_eq!(app.search_results, vec![0]);
    }

    #[test]
    fn test_update_search_case_insensitive() {
        let mut app = make_app();
        app.search_query = "ALICE".to_string();
        app.update_search();
        assert_eq!(app.search_results, vec![0]);
    }

    #[test]
    fn test_update_search_no_matches() {
        let mut app = make_app();
        app.search_query = "xyz".to_string();
        app.update_search();
        assert!(app.search_results.is_empty());
    }

    #[test]
    fn test_update_filter_finds_matches() {
        let mut app = make_app();
        app.filters = vec![(0, "Bob".to_string())];
        app.update_filter();
        assert_eq!(app.view.height(), 1);
    }

    #[test]
    fn test_autofit_uses_data_width() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.autofit_selected_column();
        // "name" col: max("Alice"=5, "Bob"=3, "Charlie"=7) = 7, header "name" = 4
        assert_eq!(app.column_widths[0], 7);
    }

    #[test]
    fn test_autofit_accounts_for_groupby_marker() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key(); // adds [K] to header: "name [K]" = 8 chars
        app.autofit_selected_column();
        // header "name [K]" = 8 chars > data max 7 → width should be 8
        assert_eq!(app.column_widths[0], 8);
    }

    #[test]
    fn test_sort_by_column_ascending() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.sort_by_column();
        assert_eq!(get_str(&app, "name", 0), "Alice");
        assert_eq!(get_str(&app, "name", 1), "Bob");
        assert_eq!(get_str(&app, "name", 2), "Charlie");
    }

    #[test]
    fn test_sort_by_column_toggles_descending() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.sort_by_column();
        app.sort_by_column();
        assert_eq!(get_str(&app, "name", 0), "Charlie");
        assert_eq!(get_str(&app, "name", 1), "Bob");
        assert_eq!(get_str(&app, "name", 2), "Alice");
    }

    #[test]
    fn test_empty_dataframe_new() {
        let df = DataFrame::empty();
        let app = App::new(df, "empty.csv".to_string());
        assert!(app.state.selected().is_none());
        assert!(app.state.selected_column().is_none());
        assert!(app.headers.is_empty());
    }

    #[test]
    fn test_update_search_on_empty_view() {
        let df = df! {
            "name" => ["Alice", "Bob"],
            "age"  => [30i64, 25],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        // Filter to zero rows then search — must not panic
        app.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        app.search_query = "alice".to_string();
        app.update_search();
        assert!(app.search_results.is_empty());
    }

    #[test]
    fn test_compute_stats_empty_view() {
        let df = df! {
            "val" => [1i64, 2, 3],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        // Should return default stats without panicking
        let stats = app.compute_stats(0);
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_filter_to_zero_rows() {
        let mut app = make_app();
        app.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        assert_eq!(app.view.height(), 0);
    }

    #[test]
    fn test_autofit_all_columns() {
        let mut app = make_app();
        app.autofit_all_columns();
        // "name" col: max("Alice"=5, "Bob"=3, "Charlie"=7) = 7
        assert_eq!(app.column_widths[0], 7);
        // "age" col: max("30"=2, "25"=2, "35"=2) = 3, header "age" = 3
        assert_eq!(app.column_widths[1], 3);
    }

    #[test]
    fn test_search_after_sort_not_stale() {
        let mut app = make_app();
        app.search_query = "alice".to_string();
        app.update_search();
        let results_before = app.search_results.clone();
        assert!(!results_before.is_empty());
        // Sort descending — Alice moves to row 2
        app.state.select_column(Some(0));
        app.sort_by_column();
        app.sort_by_column();
        // Search results should be re-computed to point to the new row index
        assert!(!app.search_results.is_empty());
        assert_ne!(app.search_results, results_before);
    }
}

#[cfg(test)]
mod columns_view_tests {
    use super::*;

    #[test]
    fn test_build_columns_profile_numeric() {
        let df = df! {
            "val" => [1i64, 2, 3],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.build_columns_profile();
        let p = &app.columns_profile[0];
        assert_eq!(p.name, "val");
        assert_eq!(p.count, 3);
        assert_eq!(p.null_count, 0);
        assert!(p.mean.is_some());
        assert!(p.median.is_some());
    }

    #[test]
    fn test_build_columns_profile_string_no_stats() {
        let df = df! {
            "name" => ["a", "b", "c"],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.build_columns_profile();
        let p = &app.columns_profile[0];
        assert!(p.mean.is_none());
        assert!(p.median.is_none());
    }
}

#[cfg(test)]
mod groupby_tests {
    use super::*;

    fn make_app() -> App {
        let df = df! {
            "dept" => ["eng", "eng", "hr"],
            "sal"  => [100i64, 200, 150],
        }
        .unwrap();
        App::new(df, "test.csv".to_string())
    }

    #[test]
    fn test_apply_groupby_aggregates() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key(); // dept as key
        app.state.select_column(Some(1));
        app.cycle_groupby_agg(); // sal → Sum
        app.apply_groupby();
        assert!(app.groupby_active);
        assert_eq!(app.view.height(), 2); // eng, hr
    }

    #[test]
    fn test_clear_groupby_restores_view() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(1));
        app.cycle_groupby_agg();
        app.apply_groupby();
        app.clear_groupby();
        assert!(!app.groupby_active);
        assert_eq!(app.view.height(), 3);
        assert_eq!(app.headers[0], "dept");
    }
}

#[cfg(test)]
mod plot_tests {
    use super::*;

    #[test]
    fn test_extract_plot_basic() {
        let df = df! {
            "x" => [1i32, 2i32, 3i32],
            "y" => [10i32, 20i32, 30i32],
        }.unwrap();
        let app = App::new(df, "test.csv".to_string());
        let (data, x_is_categorical) = crate::ui::extract_plot_data_pub(&app, 0, 1);
        assert!(!data.is_empty(), "both numeric: data should not be empty");
        assert_eq!(data.len(), 3);
        assert_eq!(data[0], (1.0, 10.0));
        assert!(!x_is_categorical, "numeric x: not categorical");
    }

    #[test]
    fn test_extract_plot_string_x() {
        let df = df! {
            "name" => ["alpha", "beta", "gamma"],
            "qty"  => [10i32, 20i32, 30i32],
        }.unwrap();
        let app = App::new(df, "test.csv".to_string());
        let (data, x_is_categorical) = crate::ui::extract_plot_data_pub(&app, 0, 1);
        assert!(!data.is_empty(), "string x: should use row index");
        assert_eq!(data[0], (0.0, 10.0));
        assert!(x_is_categorical, "string x: should be categorical");
    }
}
