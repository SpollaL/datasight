use crate::config;
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

#[derive(Debug)]
pub enum Mode {
    Search,
    Normal,
    Filter,
    PlotPickX,
    Plot,
    ColumnsView,
    UniqueValues,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum PlotType {
    #[default]
    Line,
    Bar,
    Histogram,
}

#[derive(Debug, Default)]
pub enum SortDirection {
    #[default]
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

#[derive(Default, Clone)]
pub struct ColumnStats {
    pub count: usize,
    pub min: String,
    pub max: String,
    pub mean: Option<f64>,
    pub median: Option<f64>,
}

// --- State sub-structs ---

#[derive(Default)]
pub struct SearchState {
    pub query: String,
    pub results: Vec<usize>,
    pub cursor: usize,
}

#[derive(Default)]
pub struct FilterState {
    pub filters: Vec<(usize, String)>,
    pub input: String,
    pub error: Option<String>,
    pub col: Option<usize>,
}

#[derive(Default)]
pub struct SortState {
    pub column: Option<usize>,
    pub direction: SortDirection,
    pub error: Option<String>,
}

#[derive(Default)]
pub struct GroupByState {
    pub keys: Vec<usize>,
    pub aggs: HashMap<usize, AggFunc>,
    pub active: bool,
    pub saved_headers: Vec<String>,
    pub saved_column_widths: Vec<u16>,
}

#[derive(Default)]
pub struct PlotState {
    pub y_col: Option<usize>,
    pub x_col: Option<usize>,
    pub plot_type: PlotType,
}

#[derive(Default)]
pub struct UniqueValuesState {
    pub values: Vec<(String, usize)>,
    pub filtered: Vec<(String, usize)>,
    pub query: String,
    pub state: TableState,
    pub col: usize,
    pub truncated: bool,
}

#[derive(Default)]
pub struct ColumnsViewState {
    pub profile: Vec<ColumnProfile>,
    pub state: TableState,
}

#[derive(Default)]
pub struct ViewportState {
    pub row: usize,
    pub col: usize,
}

// --- App ---

pub struct App {
    pub df: DataFrame,        // original data
    pub view: DataFrame,      // current filtered/sorted result
    pub headers: Vec<String>, // column names for display
    pub state: TableState,
    pub should_quit: bool,
    pub file_path: String,
    pub column_widths: Vec<u16>,
    pub mode: Mode,
    pub show_stats: bool,
    pub show_help: bool,
    pub help_scroll: u16,
    pub cached_stats: Option<(usize, ColumnStats)>,
    pub search: SearchState,
    pub filter: FilterState,
    pub sort: SortState,
    pub groupby: GroupByState,
    pub plot: PlotState,
    pub unique_values: UniqueValuesState,
    pub columns_view: ColumnsViewState,
    pub viewport: ViewportState,
}

/// Strips a leading comparison operator from `query`.
/// Returns `(op, rest)` where `op` is one of `">="`, `"<="`, `"!="`, `">"`, `"<"`, `"="`,
/// or `""` (no operator found), and `rest` is the trimmed remainder of the string.
/// Two-character operators are checked before their single-character prefixes.
fn parse_operator(query: &str) -> (&'static str, &str) {
    let q = query.trim();
    if let Some(r) = q.strip_prefix(">=") {
        return (">=", r.trim());
    }
    if let Some(r) = q.strip_prefix("<=") {
        return ("<=", r.trim());
    }
    if let Some(r) = q.strip_prefix("!=") {
        return ("!=", r.trim());
    }
    if let Some(r) = q.strip_prefix('>') {
        return (">", r.trim());
    }
    if let Some(r) = q.strip_prefix('<') {
        return ("<", r.trim());
    }
    if let Some(r) = q.strip_prefix('=') {
        return ("=", r.trim());
    }
    ("", q)
}

/// Build a polars filter expression for a column and query string.
/// Supports comparison operators (>, <, >=, <=, =, !=) for numeric values.
/// Falls back to case-insensitive substring matching for everything else.
/// Returns true when the query is just an operator with no value yet (user still typing).
fn is_incomplete_operator(query: &str) -> bool {
    let (op, rest) = parse_operator(query);
    !op.is_empty() && rest.is_empty()
}

/// Returns an error string if `query` uses a numeric-only operator (>, <, >=, <=)
/// with a non-numeric value or against a non-numeric column.
fn validate_filter_query(query: &str, col_name: &str, df: &DataFrame) -> Option<String> {
    let (op, rest) = parse_operator(query);
    if !matches!(op, ">" | "<" | ">=" | "<=") {
        return None;
    }
    // Numeric operators require a numeric value
    if !rest.is_empty() && rest.parse::<f64>().is_err() {
        return Some(format!("'{}' requires a number (got '{}')", op, rest));
    }
    // Numeric operators require a numeric column
    let is_numeric_col = df
        .column(col_name)
        .ok()
        .and_then(|c| c.as_series())
        .map(|s| s.dtype().is_primitive_numeric())
        .unwrap_or(false);
    if !is_numeric_col {
        return Some(format!("'{}' can only filter numeric columns", op));
    }
    None
}

fn build_filter_expr(col_name: &str, query: &str) -> Expr {
    let (op, rest) = parse_operator(query);

    if !op.is_empty() {
        if let Ok(value) = rest.parse::<f64>() {
            return match op {
                ">=" => col(col_name).gt_eq(lit(value)),
                "<=" => col(col_name).lt_eq(lit(value)),
                "!=" => col(col_name).neq(lit(value)),
                ">" => col(col_name).gt(lit(value)),
                "<" => col(col_name).lt(lit(value)),
                _ => col(col_name).eq(lit(value)),
            };
        }
        // Non-numeric value with = / != : exact string match.
        // "(null)" is a sentinel for actual null values (not the string "null").
        if op == "=" {
            if rest == "(null)" {
                return col(col_name).is_null();
            }
            return col(col_name)
                .cast(DataType::String)
                .eq(lit(rest.to_string()));
        }
        if op == "!=" {
            if rest == "(null)" {
                return col(col_name).is_not_null();
            }
            return col(col_name)
                .cast(DataType::String)
                .neq(lit(rest.to_string()));
        }
    }

    col(col_name)
        .cast(DataType::String)
        .str()
        .contains(lit(query), false)
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
            df,
            view,
            headers,
            state: TableState::default(),
            should_quit: false,
            file_path,
            column_widths: vec![config::DEFAULT_COLUMN_WIDTH; column_count],
            mode: Mode::Normal,
            show_stats: false,
            show_help: false,
            help_scroll: 0,
            cached_stats: None,
            search: SearchState::default(),
            filter: FilterState::default(),
            sort: SortState::default(),
            groupby: GroupByState::default(),
            plot: PlotState::default(),
            unique_values: UniqueValuesState::default(),
            columns_view: ColumnsViewState::default(),
            viewport: ViewportState::default(),
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
            self.search.results.clear();
            return;
        }
        let col_name = &self.headers[current_column];
        let query = self.search.query.to_lowercase();
        let Some(series) = self
            .view
            .column(col_name)
            .ok()
            .and_then(|c| c.as_series())
            .and_then(|s| s.cast(&DataType::String).ok())
        else {
            self.search.results.clear();
            return;
        };
        self.search.results = series
            .str()
            .map(|ca| {
                ca.into_iter()
                    .enumerate()
                    .filter(|(_, v)| v.is_some_and(|s| s.to_lowercase().contains(&query)))
                    .map(|(i, _)| i)
                    .collect()
            })
            .unwrap_or_default();
        self.search.cursor = 0;
    }

    pub fn update_filter(&mut self) {
        self.cached_stats = None;
        let mut mask = lit(true);
        for (colidx, query) in &self.filter.filters {
            let col_name = &self.headers[*colidx];
            mask = mask.and(build_filter_expr(col_name, query));
        }
        if !self.filter.input.is_empty() && !is_incomplete_operator(&self.filter.input) {
            let col_idx = self
                .filter
                .col
                .unwrap_or_else(|| self.state.selected_column().unwrap_or(0))
                .min(self.headers.len().saturating_sub(1));
            let col_name = self.headers[col_idx].clone();
            self.filter.error = validate_filter_query(&self.filter.input, &col_name, &self.df);
            if self.filter.error.is_none() {
                mask = mask.and(build_filter_expr(&col_name, &self.filter.input));
            }
        } else {
            self.filter.error = None;
        }
        let filtered = match self.df.clone().lazy().filter(mask).collect() {
            Ok(df) => df,
            Err(e) => {
                self.filter.error = Some(format!("Filter error: {}", e));
                self.df.clone()
            }
        };

        self.viewport.row = 0;
        self.view = if let Some(sort_col) = self.sort.column {
            let col_name = &self.headers[sort_col];
            let opts = SortMultipleOptions::default()
                .with_order_descending(matches!(self.sort.direction, SortDirection::Descending));
            match filtered.sort([col_name], opts) {
                Ok(sorted) => sorted,
                Err(_) => filtered,
            }
        } else {
            filtered
        };
        if !self.search.query.is_empty() {
            self.update_search();
        }
    }

    pub fn sort_by_column(&mut self) {
        self.cached_stats = None;
        let current_column = self.state.selected_column().unwrap_or(0);
        if self.sort.column == Some(current_column) {
            self.sort.direction = match self.sort.direction {
                SortDirection::Ascending => SortDirection::Descending,
                SortDirection::Descending => SortDirection::Ascending,
            };
        } else {
            self.sort.column = Some(current_column);
            self.sort.direction = SortDirection::Ascending;
        }
        let col_name = &self.headers[current_column];
        let opts = SortMultipleOptions::default()
            .with_order_descending(matches!(self.sort.direction, SortDirection::Descending));
        self.view = match self.view.sort([col_name], opts) {
            Ok(sorted) => {
                self.sort.error = None;
                sorted
            }
            Err(e) => {
                self.sort.error = Some(format!("Sort error: {}", e));
                self.view.clone()
            }
        };
        if !self.search.query.is_empty() {
            self.update_search();
        }
    }

    fn compute_column_width(&self, col_idx: usize) -> u16 {
        let header_width = self.header_label(col_idx).chars().count() as u16;
        let max_data = self
            .view
            .column(&self.headers[col_idx])
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
        max_data
            .max(header_width)
            .clamp(config::MIN_COLUMN_WIDTH, config::MAX_COLUMN_WIDTH)
    }

    pub fn select_next_row(&mut self) {
        let max = self.view.height().saturating_sub(1);
        let next = self.state.selected().map_or(0, |r| (r + 1).min(max));
        self.state.select(Some(next));
    }

    pub fn select_previous_row(&mut self) {
        let prev = self.state.selected().map_or(0, |r| r.saturating_sub(1));
        self.state.select(Some(prev));
    }

    pub fn select_first_row(&mut self) {
        self.state.select(Some(0));
    }

    pub fn select_last_row(&mut self) {
        let last = self.view.height().saturating_sub(1);
        self.state.select(Some(last));
    }

    pub fn scroll_down_rows(&mut self, amount: u16) {
        let max = self.view.height().saturating_sub(1);
        let next = self
            .state
            .selected()
            .map_or(0, |r| (r + amount as usize).min(max));
        self.state.select(Some(next));
    }

    pub fn scroll_up_rows(&mut self, amount: u16) {
        let prev = self
            .state
            .selected()
            .map_or(0, |r| r.saturating_sub(amount as usize));
        self.state.select(Some(prev));
    }

    pub fn select_next_column(&mut self) {
        let max = self.headers.len().saturating_sub(1);
        let next = self.state.selected_column().map_or(0, |c| (c + 1).min(max));
        self.state.select_column(Some(next));
    }

    pub fn select_previous_column(&mut self) {
        let prev = self
            .state
            .selected_column()
            .map_or(0, |c| c.saturating_sub(1));
        self.state.select_column(Some(prev));
    }

    pub fn autofit_selected_column(&mut self) {
        if let Some(col_idx) = self.state.selected_column() {
            self.column_widths[col_idx] = self.compute_column_width(col_idx);
        }
    }

    pub fn autofit_all_columns(&mut self) {
        for col_idx in 0..self.headers.len() {
            self.column_widths[col_idx] = self.compute_column_width(col_idx);
        }
    }

    pub fn header_label(&self, col_idx: usize) -> String {
        let base = &self.headers[col_idx];
        let label = if self.sort.column == Some(col_idx) {
            let dir = if matches!(self.sort.direction, SortDirection::Descending) {
                "▼"
            } else {
                "▲"
            };
            format!("{} {}", base, dir)
        } else {
            base.clone()
        };
        if self.groupby.keys.contains(&col_idx) {
            format!("{} [K]", label)
        } else if let Some(func) = self.groupby.aggs.get(&col_idx) {
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
        ColumnStats {
            count,
            min,
            max,
            mean,
            median,
        }
    }

    pub fn get_or_compute_stats(&mut self, col: usize) -> ColumnStats {
        if let Some((cached_col, ref stats)) = self.cached_stats {
            if cached_col == col {
                return stats.clone();
            }
        }
        let stats = self.compute_stats(col);
        self.cached_stats = Some((col, stats.clone()));
        stats
    }

    pub fn toggle_groupby_key(&mut self) {
        let col = self.state.selected_column().unwrap_or(0);
        if let Some(pos) = self.groupby.keys.iter().position(|&k| k == col) {
            self.groupby.keys.remove(pos);
        } else {
            self.groupby.keys.push(col);
            self.groupby.aggs.remove(&col);
        }
    }

    pub fn cycle_groupby_agg(&mut self) {
        let col = self.state.selected_column().unwrap_or(0);
        if self.groupby.keys.contains(&col) {
            return;
        };
        let next = match self.groupby.aggs.get(&col) {
            None => Some(AggFunc::Sum),
            Some(AggFunc::Sum) => Some(AggFunc::Mean),
            Some(AggFunc::Mean) => Some(AggFunc::Count),
            Some(AggFunc::Count) => Some(AggFunc::Min),
            Some(AggFunc::Min) => Some(AggFunc::Max),
            Some(AggFunc::Max) => None,
        };
        match next {
            Some(f) => {
                self.groupby.aggs.insert(col, f);
            }
            None => {
                self.groupby.aggs.remove(&col);
            }
        };
    }

    pub fn apply_groupby(&mut self) {
        self.cached_stats = None;
        if self.groupby.keys.is_empty() || self.groupby.aggs.is_empty() {
            return;
        }
        let key_exprs: Vec<Expr> = self
            .groupby
            .keys
            .iter()
            .map(|&i| col(&self.headers[i]))
            .collect();
        let agg_exprs: Vec<Expr> = self
            .groupby
            .aggs
            .iter()
            .map(|(i, func)| {
                let name = &self.headers[*i];
                match func {
                    AggFunc::Sum => col(name).sum().alias(format!("{}_sum", name)),
                    AggFunc::Mean => col(name).mean().alias(format!("{}_mean", name)),
                    AggFunc::Count => col(name).count().alias(format!("{}_count", name)),
                    AggFunc::Min => col(name).min().alias(format!("{}_min", name)),
                    AggFunc::Max => col(name).max().alias(format!("{}_max", name)),
                }
            })
            .collect();
        let first_key = self.headers[self.groupby.keys[0]].clone();
        let result = self
            .view
            .clone()
            .lazy()
            .group_by(key_exprs)
            .agg(agg_exprs)
            .sort([&first_key], SortMultipleOptions::default())
            .collect();
        if let Ok(df) = result {
            self.viewport.row = 0;
            self.groupby.saved_headers = self.headers.clone();
            self.groupby.saved_column_widths = self.column_widths.clone();
            self.headers = df
                .get_column_names()
                .iter()
                .map(|s| s.to_string())
                .collect();
            self.column_widths = vec![config::DEFAULT_COLUMN_WIDTH; df.width()];
            self.sort.column = None;
            self.search.results = Vec::new();
            self.search.cursor = 0;
            self.view = df;
            self.groupby.active = true;
            self.state.select(Some(0));
            self.state.select_column(Some(0));
        }
    }

    pub fn build_unique_values(&mut self) {
        let col_idx = self.state.selected_column().unwrap_or(0);
        self.unique_values.col = col_idx;
        self.unique_values.query = String::new();

        let counts: Vec<(String, usize)> = (|| {
            let s = self
                .view
                .column(&self.headers[col_idx])
                .ok()?
                .as_series()?
                .clone();
            let str_s = s.cast(&DataType::String).ok()?;
            let ca = str_s.str().ok()?.clone();
            // Use Option<String> keys so actual nulls and the string "null" are
            // counted separately. None is displayed as "(null)" below.
            let mut map: HashMap<Option<String>, usize> = HashMap::new();
            for v in ca.into_iter() {
                *map.entry(v.map(|s| s.to_string())).or_insert(0) += 1;
            }
            let mut pairs: Vec<(String, usize)> = map
                .into_iter()
                .map(|(k, v)| (k.unwrap_or_else(|| "(null)".to_string()), v))
                .collect();
            pairs.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            let truncated = pairs.len() > config::MAX_UNIQUE;
            pairs.truncate(config::MAX_UNIQUE);
            self.unique_values.truncated = truncated;
            Some(pairs)
        })()
        .unwrap_or_default();

        self.unique_values.values = counts;
        self.unique_values.filtered = self.unique_values.values.clone();
        self.unique_values.state = TableState::default();
        if !self.unique_values.filtered.is_empty() {
            self.unique_values.state.select(Some(0));
        }
    }

    pub fn filter_unique_values(&mut self) {
        let q = self.unique_values.query.to_lowercase();
        self.unique_values.filtered = if q.is_empty() {
            self.unique_values.values.clone()
        } else {
            self.unique_values
                .values
                .iter()
                .filter(|(v, _)| v.to_lowercase().contains(&q))
                .cloned()
                .collect()
        };
        self.unique_values
            .state
            .select(if self.unique_values.filtered.is_empty() {
                None
            } else {
                Some(0)
            });
    }

    pub fn build_columns_profile(&mut self) {
        self.columns_view.profile = self
            .view
            .get_columns()
            .iter()
            .map(|col| {
                let name = col.name().to_string();
                let dtype = col.dtype().to_string();
                let count = col.len();
                let null_count = col.null_count();
                let unique = col.as_series().and_then(|s| s.n_unique().ok()).unwrap_or(0);
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
                ColumnProfile {
                    name,
                    dtype,
                    count,
                    null_count,
                    unique,
                    min,
                    max,
                    mean,
                    median,
                }
            })
            .collect();
        self.columns_view.state.select(Some(0));
    }

    pub fn plot_type_label(&self) -> &str {
        match self.plot.plot_type {
            PlotType::Line => "Line",
            PlotType::Bar => "Bar",
            PlotType::Histogram => "Histogram",
        }
    }

    pub fn clear_groupby(&mut self) {
        if !self.groupby.active {
            return;
        }
        self.cached_stats = None;
        self.viewport.row = 0;
        self.headers = self.groupby.saved_headers.clone();
        self.column_widths = self.groupby.saved_column_widths.clone();
        self.groupby.keys = Vec::new();
        self.groupby.aggs = HashMap::new();
        self.groupby.active = false;
        self.sort.column = None;
        self.search.results = Vec::new();
        self.search.cursor = 0;
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
        app.search.query = "alice".to_string();
        app.update_search();
        assert_eq!(app.search.results, vec![0]);
    }

    #[test]
    fn test_update_search_case_insensitive() {
        let mut app = make_app();
        app.search.query = "ALICE".to_string();
        app.update_search();
        assert_eq!(app.search.results, vec![0]);
    }

    #[test]
    fn test_update_search_no_matches() {
        let mut app = make_app();
        app.search.query = "xyz".to_string();
        app.update_search();
        assert!(app.search.results.is_empty());
    }

    #[test]
    fn test_update_filter_finds_matches() {
        let mut app = make_app();
        app.filter.filters = vec![(0, "Bob".to_string())];
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
        app.filter.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        app.search.query = "alice".to_string();
        app.update_search();
        assert!(app.search.results.is_empty());
    }

    #[test]
    fn test_compute_stats_empty_view() {
        let df = df! {
            "val" => [1i64, 2, 3],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.filter.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        // Should return default stats without panicking
        let stats = app.compute_stats(0);
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_filter_to_zero_rows() {
        let mut app = make_app();
        app.filter.filters = vec![(0, "zzznomatch".to_string())];
        app.update_filter();
        assert_eq!(app.view.height(), 0);
    }

    #[test]
    fn test_autofit_all_columns() {
        let mut app = make_app();
        app.autofit_all_columns();
        // "name" col: max("Alice"=5, "Bob"=3, "Charlie"=7) = 7
        assert_eq!(app.column_widths[0], 7);
        // "age" col: max("30"=2, "25"=2, "35"=2) = 3, header "age" = 3 → clamped to config::MIN_COLUMN_WIDTH=6
        assert_eq!(app.column_widths[1], 6);
    }

    #[test]
    fn test_search_after_sort_not_stale() {
        let mut app = make_app();
        app.search.query = "alice".to_string();
        app.update_search();
        let results_before = app.search.results.clone();
        assert!(!results_before.is_empty());

        // Sort descending — Alice moves to row 2
        app.state.select_column(Some(0));
        app.sort_by_column();
        app.sort_by_column();
        // Search results should be re-computed to point to the new row index
        assert!(!app.search.results.is_empty());
        assert_ne!(app.search.results, results_before);
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
        let p = &app.columns_view.profile[0];
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
        let p = &app.columns_view.profile[0];
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
        assert!(app.groupby.active);
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
        assert!(!app.groupby.active);
        assert_eq!(app.view.height(), 3);
        assert_eq!(app.headers[0], "dept");
    }
}

#[cfg(test)]
mod filter_expr_tests {
    use super::*;

    fn make_app() -> App {
        let df = df! {
            "name" => ["Alice", "Bob", "Charlie"],
            "age"  => [18i64, 25, 30],
        }
        .unwrap();
        App::new(df, "test.csv".to_string())
    }

    fn apply(app: &mut App, col_idx: usize, query: &str) -> usize {
        app.filter.filters = vec![(col_idx, query.to_string())];
        app.update_filter();
        app.view.height()
    }

    #[test]
    fn test_gt() {
        assert_eq!(apply(&mut make_app(), 1, "> 18"), 2); // Bob, Charlie
    }

    #[test]
    fn test_lt() {
        assert_eq!(apply(&mut make_app(), 1, "< 25"), 1); // Alice
    }

    #[test]
    fn test_gte() {
        assert_eq!(apply(&mut make_app(), 1, ">= 25"), 2); // Bob, Charlie
    }

    #[test]
    fn test_lte() {
        assert_eq!(apply(&mut make_app(), 1, "<= 25"), 2); // Alice, Bob
    }

    #[test]
    fn test_eq() {
        assert_eq!(apply(&mut make_app(), 1, "= 25"), 1); // Bob
    }

    #[test]
    fn test_neq() {
        assert_eq!(apply(&mut make_app(), 1, "!= 25"), 2); // Alice, Charlie
    }

    #[test]
    fn test_fallback_to_substring_for_strings() {
        assert_eq!(apply(&mut make_app(), 0, "li"), 2); // Alice, Charlie
    }

    #[test]
    fn test_non_numeric_value_falls_back_to_substring() {
        // "> abc" can't parse as f64, so falls back to substring — no match
        assert_eq!(apply(&mut make_app(), 1, "> abc"), 0);
    }

    #[test]
    fn test_spaces_around_operator_and_value() {
        assert_eq!(apply(&mut make_app(), 1, "  >  18  "), 2);
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
        }
        .unwrap();
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
        }
        .unwrap();
        let app = App::new(df, "test.csv".to_string());
        let (data, x_is_categorical) = crate::ui::extract_plot_data_pub(&app, 0, 1);
        assert!(!data.is_empty(), "string x: should use row index");
        assert_eq!(data[0], (0.0, 10.0));
        assert!(x_is_categorical, "string x: should be categorical");
    }
}

#[cfg(test)]
mod parse_operator_tests {
    use super::*;

    #[test]
    fn test_gte() {
        assert_eq!(parse_operator(">= 5"), (">=", "5"));
    }

    #[test]
    fn test_lte() {
        assert_eq!(parse_operator("<= 5"), ("<=", "5"));
    }

    #[test]
    fn test_neq() {
        assert_eq!(parse_operator("!= 5"), ("!=", "5"));
    }

    #[test]
    fn test_gt() {
        assert_eq!(parse_operator("> 5"), (">", "5"));
    }

    #[test]
    fn test_lt() {
        assert_eq!(parse_operator("< 5"), ("<", "5"));
    }

    #[test]
    fn test_eq() {
        assert_eq!(parse_operator("= 5"), ("=", "5"));
    }

    #[test]
    fn test_no_op() {
        assert_eq!(parse_operator("hello"), ("", "hello"));
    }

    #[test]
    fn test_trims_leading_whitespace() {
        assert_eq!(parse_operator("  >= 10"), (">=", "10"));
    }

    #[test]
    fn test_trims_value_whitespace() {
        assert_eq!(parse_operator(">  42  "), (">", "42"));
    }

    // Ensure two-char operators are not mis-parsed as one-char operators.
    #[test]
    fn test_gte_not_parsed_as_gt() {
        let (op, _) = parse_operator(">= 5");
        assert_eq!(op, ">=");
    }
}

#[cfg(test)]
mod incomplete_operator_tests {
    use super::*;

    #[test]
    fn test_bare_gt_is_incomplete() {
        assert!(is_incomplete_operator(">"));
    }

    #[test]
    fn test_bare_gte_is_incomplete() {
        assert!(is_incomplete_operator(">="));
    }

    #[test]
    fn test_bare_neq_is_incomplete() {
        assert!(is_incomplete_operator("!="));
    }

    #[test]
    fn test_operator_with_value_is_not_incomplete() {
        assert!(!is_incomplete_operator("> 5"));
    }

    #[test]
    fn test_plain_text_is_not_incomplete() {
        assert!(!is_incomplete_operator("hello"));
    }

    #[test]
    fn test_empty_string_is_not_incomplete() {
        assert!(!is_incomplete_operator(""));
    }
}

#[cfg(test)]
mod validate_filter_tests {
    use super::*;

    fn make_df() -> DataFrame {
        df! {
            "name" => ["Alice", "Bob"],
            "age"  => [25i64, 30],
        }
        .unwrap()
    }

    #[test]
    fn test_numeric_op_on_numeric_col_valid() {
        assert!(validate_filter_query("> 20", "age", &make_df()).is_none());
    }

    #[test]
    fn test_numeric_op_on_string_col_returns_error() {
        let err = validate_filter_query("> 5", "name", &make_df());
        assert!(err.is_some());
        assert!(err.unwrap().contains("numeric"));
    }

    #[test]
    fn test_numeric_op_with_non_numeric_value_returns_error() {
        let err = validate_filter_query("> abc", "age", &make_df());
        assert!(err.is_some());
        assert!(err.unwrap().contains("number"));
    }

    #[test]
    fn test_eq_op_on_string_col_is_valid() {
        // = and != are string-compatible, so no error
        assert!(validate_filter_query("= Alice", "name", &make_df()).is_none());
    }

    #[test]
    fn test_substring_query_is_valid() {
        assert!(validate_filter_query("Ali", "name", &make_df()).is_none());
    }
}

#[cfg(test)]
mod chained_filter_tests {
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
    fn test_two_filters_on_different_columns() {
        let mut app = make_app();
        // dept = eng AND sal > 150 → only the 200 row
        app.filter.filters = vec![(0, "eng".to_string()), (1, "> 150".to_string())];
        app.update_filter();
        assert_eq!(app.view.height(), 1);
    }

    #[test]
    fn test_duplicate_filter_not_stacked() {
        let mut app = make_app();
        app.filter.filters = vec![(0, "eng".to_string())];
        app.update_filter();
        let height_first = app.view.height();

        // Simulate pressing Enter with the same filter again
        let col = 0;
        let query = "eng".to_string();
        let already_exists = app
            .filter
            .filters
            .iter()
            .any(|(c, q)| *c == col && q == &query);
        if !already_exists {
            app.filter.filters.push((col, query));
            app.update_filter();
        }

        assert_eq!(app.view.height(), height_first);
        assert_eq!(app.filter.filters.len(), 1);
    }

    #[test]
    fn test_range_filter_two_ops_same_column() {
        let mut app = make_app();
        // sal >= 100 AND sal <= 150 → 100, 150 rows
        app.filter.filters = vec![(1, ">= 100".to_string()), (1, "<= 150".to_string())];
        app.update_filter();
        assert_eq!(app.view.height(), 2);
    }

    #[test]
    fn test_filter_error_set_for_numeric_op_on_string_col() {
        let mut app = make_app();
        app.state.select_column(Some(0)); // dept (string)
        app.filter.input = "> 5".to_string();
        app.update_filter();
        assert!(app.filter.error.is_some());
    }
}

#[cfg(test)]
mod stats_tests {
    use super::*;

    #[test]
    fn test_compute_stats_values() {
        let df = df! {
            "val" => [10i64, 20, 30],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        let stats = app.compute_stats(0);
        assert_eq!(stats.count, 3);
        assert_eq!(stats.min, "10");
        assert_eq!(stats.max, "30");
        assert!((stats.mean.unwrap() - 20.0).abs() < 1e-9);
        assert!((stats.median.unwrap() - 20.0).abs() < 1e-9);
    }

    #[test]
    fn test_compute_stats_out_of_bounds_col() {
        let df = df! { "val" => [1i64] }.unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        // col index 99 is out of bounds — should return default without panic
        let stats = app.compute_stats(99);
        assert_eq!(stats.count, 0);
    }
}

#[cfg(test)]
mod unique_values_tests {
    use super::*;

    fn make_app() -> App {
        let df = df! {
            "status" => ["active", "inactive", "active", "pending"],
        }
        .unwrap();
        App::new(df, "test.csv".to_string())
    }

    #[test]
    fn test_build_unique_values_sorted_by_frequency() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.build_unique_values();
        // "active" appears twice, so it should be first
        assert_eq!(app.unique_values.values[0].0, "active");
        assert_eq!(app.unique_values.values[0].1, 2);
        assert_eq!(app.unique_values.values.len(), 3);
    }

    #[test]
    fn test_build_unique_values_null_shown_as_null_label() {
        use polars::prelude::*;
        let s = Series::new(
            "name".into(),
            &[Some("Alice"), None, Some("Alice"), None, None],
        );
        let df = DataFrame::new(vec![s.into()]).unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.state.select_column(Some(0));
        app.build_unique_values();
        // nulls (3 of them) should be first (highest count)
        assert_eq!(app.unique_values.values[0].1, 3);
        assert_eq!(app.unique_values.values[0].0, "(null)");
        assert_eq!(app.unique_values.values[1].0, "Alice");
        assert_eq!(app.unique_values.values[1].1, 2);
    }

    #[test]
    fn test_build_unique_values_null_from_csv_fixture() {
        // Verify that null CSV cells round-trip through build_unique_values as "(null)"
        use polars::prelude::*;
        let df = CsvReadOptions::default()
            .with_infer_schema_length(Some(100))
            .try_into_reader_with_file_path(Some("tests/fixtures/orders_nulls.csv".into()))
            .unwrap()
            .finish()
            .unwrap();
        let mut app = App::new(df, "orders_nulls.csv".to_string());
        // customer_name is col index 3
        app.state.select_column(Some(3));
        app.build_unique_values();
        // The null entries should appear as "(null)"
        let null_entry = app.unique_values.values.iter().find(|(v, _)| v == "(null)");
        assert!(
            null_entry.is_some(),
            "Expected '(null)' in unique values, got: {:?}",
            app.unique_values.values.iter().take(5).collect::<Vec<_>>()
        );
        assert_eq!(null_entry.unwrap().1, 3);
    }

    #[test]
    fn test_filter_unique_values_narrows_list() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.build_unique_values();
        app.unique_values.query = "act".to_string();
        app.filter_unique_values();
        // "active" and "inactive" both contain "act"
        assert_eq!(app.unique_values.filtered.len(), 2);
    }

    #[test]
    fn test_filter_unique_values_case_insensitive() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.build_unique_values();
        // "PEND" lowercases to "pend", which only matches "pending"
        app.unique_values.query = "PEND".to_string();
        app.filter_unique_values();
        assert_eq!(app.unique_values.filtered.len(), 1);
        assert_eq!(app.unique_values.filtered[0].0, "pending");
    }

    #[test]
    fn test_filter_unique_values_empty_query_shows_all() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.build_unique_values();
        app.unique_values.query = "act".to_string();
        app.filter_unique_values();
        app.unique_values.query = String::new();
        app.filter_unique_values();
        assert_eq!(
            app.unique_values.filtered.len(),
            app.unique_values.values.len()
        );
    }
}

#[cfg(test)]
mod cycle_agg_tests {
    use super::*;

    fn make_app() -> App {
        let df = df! {
            "dept" => ["eng"],
            "sal"  => [100i64],
        }
        .unwrap();
        App::new(df, "test.csv".to_string())
    }

    #[test]
    fn test_cycle_agg_progresses_through_all_variants() {
        let mut app = make_app();
        app.state.select_column(Some(1)); // sal — not a key col

        app.cycle_groupby_agg();
        assert_eq!(app.groupby.aggs[&1], AggFunc::Sum);

        app.cycle_groupby_agg();
        assert_eq!(app.groupby.aggs[&1], AggFunc::Mean);

        app.cycle_groupby_agg();
        assert_eq!(app.groupby.aggs[&1], AggFunc::Count);

        app.cycle_groupby_agg();
        assert_eq!(app.groupby.aggs[&1], AggFunc::Min);

        app.cycle_groupby_agg();
        assert_eq!(app.groupby.aggs[&1], AggFunc::Max);

        // One more cycle removes the agg entirely
        app.cycle_groupby_agg();
        assert!(!app.groupby.aggs.contains_key(&1));
    }

    #[test]
    fn test_cycle_agg_no_op_on_key_column() {
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key(); // col 0 is now a key
        app.cycle_groupby_agg(); // should be a no-op
        assert!(!app.groupby.aggs.contains_key(&0));
    }
}
