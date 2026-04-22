//! Application state and data-manipulation logic.
//!
//! [`App`] owns two DataFrames: `df` (the original, never mutated after load) and
//! `view` (the current filtered/sorted/grouped result shown in the UI).
//! State is decomposed into focused sub-structs ([`SearchState`], [`FilterState`],
//! [`SortState`], [`GroupByState`], [`PlotState`], [`UniqueValuesState`],
//! [`ColumnsViewState`], [`ViewportState`]) so each concern is self-contained.
//!
//! The [`Mode`] enum is the central state-machine discriminant: [`crate::events`]
//! dispatches key events based on the current mode, and [`crate::ui`] branches on
//! mode to choose what to render.

use crate::config;
use polars::prelude::*;
use ratatui::widgets::TableState;
use std::collections::{HashMap, HashSet};

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
    PlotPickY,
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

#[derive(Debug, Default, Clone)]
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
    // Keyed by column name (not index) so filters remain meaningful
    // across groupby transitions, which rewrite `App::headers`.
    pub filters: Vec<(String, String)>,
    pub query: String,
    pub error: Option<String>,
    pub col: Option<usize>,
}

#[derive(Default)]
pub struct SortState {
    pub sorts: Vec<(usize, SortDirection)>, // ordered: index 0 = primary sort
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
    pub y_cols: Vec<usize>,
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

/// Parsed representation of a filter input string.
///
/// `FilterQuery::parse` returns `None` when the user has typed only an operator
/// with no value yet (e.g. `">"`), so callers can suppress error display while
/// the user is still typing. A `Some(FilterQuery)` is ready to validate and
/// build a Polars expression.
struct FilterQuery<'a> {
    op: &'static str, // "", ">", "<", ">=", "<=", "!=", "="
    rest: &'a str,    // value portion after the operator
    raw: &'a str,     // original input (needed for substring fallback)
}

impl<'a> FilterQuery<'a> {
    /// Parse an input string. Returns `None` if the operator is present but
    /// the value is empty (user is still typing an operator like `">"`).
    fn parse(input: &'a str) -> Option<Self> {
        let (op, rest) = parse_operator(input);
        if !op.is_empty() && rest.is_empty() {
            return None; // incomplete operator — suppress errors
        }
        Some(Self {
            op,
            rest,
            raw: input,
        })
    }

    /// Returns `Some(error_msg)` if the query is semantically invalid for
    /// the given column (e.g. a numeric operator on a string column).
    fn validate(&self, col_name: &str, df: &DataFrame) -> Option<String> {
        if !matches!(self.op, ">" | "<" | ">=" | "<=") {
            return None;
        }
        // Numeric operators require a numeric value
        if !self.rest.is_empty() && self.rest.parse::<f64>().is_err() {
            return Some(format!(
                "'{}' requires a number (got '{}')",
                self.op, self.rest
            ));
        }
        // Numeric operators require a numeric column
        let is_numeric_col = df
            .column(col_name)
            .ok()
            .and_then(|c| c.as_series())
            .map(|s| s.dtype().is_primitive_numeric())
            .unwrap_or(false);
        if !is_numeric_col {
            return Some(format!("'{}' can only filter numeric columns", self.op));
        }
        None
    }

    /// Build the Polars filter expression for this query against `col_name`.
    fn build_expr(&self, col_name: &str) -> Expr {
        if !self.op.is_empty() {
            if let Ok(value) = self.rest.parse::<f64>() {
                return match self.op {
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
            if self.op == "=" {
                if self.rest == "(null)" {
                    return col(col_name).is_null();
                }
                return col(col_name)
                    .cast(DataType::String)
                    .eq(lit(self.rest.to_string()));
            }
            if self.op == "!=" {
                if self.rest == "(null)" {
                    return col(col_name).is_not_null();
                }
                return col(col_name)
                    .cast(DataType::String)
                    .neq(lit(self.rest.to_string()));
            }
        }
        col(col_name)
            .cast(DataType::String)
            .str()
            .contains(lit(self.raw), false)
    }
}

fn apply_sorts(
    df: DataFrame,
    sorts: &[(usize, SortDirection)],
    headers: &[String],
) -> Result<DataFrame, PolarsError> {
    if sorts.is_empty() {
        return Ok(df);
    }
    let col_names: Vec<&str> = sorts.iter().map(|(i, _)| headers[*i].as_str()).collect();
    let descending: Vec<bool> = sorts
        .iter()
        .map(|(_, d)| matches!(d, SortDirection::Descending))
        .collect();
    let opts = SortMultipleOptions::default().with_order_descending_multi(descending);
    df.sort(col_names, opts)
}

/// Build a filter expression for an already-committed filter entry (col, query).
/// Committed filters are never incomplete, so we unwrap the parse result.
fn build_committed_filter_expr(col_name: &str, query: &str) -> Expr {
    FilterQuery::parse(query)
        .map(|fq| fq.build_expr(col_name))
        .unwrap_or_else(|| {
            // Fallback: plain substring match (should not occur for committed filters)
            col(col_name)
                .cast(DataType::String)
                .str()
                .contains(lit(query), false)
        })
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

    /// Rebuilds `self.view` from `self.df` through the pipeline
    /// raw → filter(raw-cols) → [groupby] → filter(agg-cols + in-progress) → sort.
    ///
    /// Committed filters are routed by column-name membership: filters whose column
    /// exists in `self.df` run pre-aggregation, the rest run post-aggregation. This
    /// lets e.g. a `region = North` filter keep shaping the raw rows that feed a
    /// groupby, while a `sal_sum > 500` filter narrows the aggregated result.
    pub fn update_filter(&mut self) {
        self.cached_stats = None;
        self.viewport.row = 0;

        let raw_cols: HashSet<String> = self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();

        // Split committed filters by which stage of the pipeline they apply to.
        let mut raw_mask = lit(true);
        let mut agg_mask = lit(true);
        for (col_name, query) in &self.filter.filters {
            let expr = build_committed_filter_expr(col_name, query);
            if raw_cols.contains(col_name) {
                raw_mask = raw_mask.and(expr);
            } else {
                agg_mask = agg_mask.and(expr);
            }
        }

        let raw_filtered = match self.df.clone().lazy().filter(raw_mask).collect() {
            Ok(df) => df,
            Err(e) => {
                self.filter.error = Some(format!("Filter error: {}", e));
                self.df.clone()
            }
        };

        let base = if self.groupby.active {
            match self.run_groupby_agg(raw_filtered.clone()) {
                Ok(df) => df,
                Err(e) => {
                    self.filter.error = Some(format!("Groupby error: {}", e));
                    raw_filtered
                }
            }
        } else {
            raw_filtered
        };

        // In-progress filter (user is still typing). It targets `self.headers`,
        // which matches `base`'s schema, so it always joins the post-agg mask.
        if !self.filter.query.is_empty() {
            match FilterQuery::parse(&self.filter.query) {
                None => {
                    // Incomplete operator (e.g. ">") — suppress errors while typing
                    self.filter.error = None;
                }
                Some(fq) => {
                    let col_idx = self
                        .filter
                        .col
                        .unwrap_or_else(|| self.state.selected_column().unwrap_or(0))
                        .min(self.headers.len().saturating_sub(1));
                    let col_name = self.headers[col_idx].clone();
                    self.filter.error = fq.validate(&col_name, &base);
                    if self.filter.error.is_none() {
                        agg_mask = agg_mask.and(fq.build_expr(&col_name));
                    }
                }
            }
        } else {
            self.filter.error = None;
        }

        let filtered = match base.clone().lazy().filter(agg_mask).collect() {
            Ok(df) => df,
            Err(e) => {
                self.filter.error = Some(format!("Filter error: {}", e));
                base
            }
        };

        self.view = if self.sort.sorts.is_empty() {
            filtered
        } else {
            let sorts = self.sort.sorts.clone();
            let headers = self.headers.clone();
            match apply_sorts(filtered.clone(), &sorts, &headers) {
                Ok(sorted) => sorted,
                Err(_) => filtered,
            }
        };
        if !self.search.query.is_empty() {
            self.update_search();
        }
    }

    /// Rebuilds the groupby aggregation from an arbitrary raw-schema base
    /// (typically the already-filtered `self.df`). Uses `groupby.saved_headers`
    /// to resolve keys/aggs so this works after `apply_groupby` has swapped in
    /// the grouped schema.
    fn run_groupby_agg(&self, base: DataFrame) -> PolarsResult<DataFrame> {
        if self.groupby.keys.is_empty() || self.groupby.aggs.is_empty() {
            return Ok(base);
        }
        let schema = &self.groupby.saved_headers;
        let key_exprs: Vec<Expr> = self.groupby.keys.iter().map(|&i| col(&schema[i])).collect();
        let agg_exprs: Vec<Expr> = self
            .groupby
            .aggs
            .iter()
            .map(|(i, func)| {
                let name = &schema[*i];
                match func {
                    AggFunc::Sum => col(name).sum().alias(format!("{}_sum", name)),
                    AggFunc::Mean => col(name).mean().alias(format!("{}_mean", name)),
                    AggFunc::Count => col(name).count().alias(format!("{}_count", name)),
                    AggFunc::Min => col(name).min().alias(format!("{}_min", name)),
                    AggFunc::Max => col(name).max().alias(format!("{}_max", name)),
                }
            })
            .collect();
        let first_key = schema[self.groupby.keys[0]].clone();
        base.lazy()
            .group_by(key_exprs)
            .agg(agg_exprs)
            .sort([&first_key], SortMultipleOptions::default())
            .collect()
    }

    pub fn sort_by_column(&mut self) {
        self.cached_stats = None;
        let current_column = self.state.selected_column().unwrap_or(0);
        if let Some(pos) = self
            .sort
            .sorts
            .iter()
            .position(|(c, _)| *c == current_column)
        {
            match self.sort.sorts[pos].1 {
                SortDirection::Ascending => self.sort.sorts[pos].1 = SortDirection::Descending,
                SortDirection::Descending => {
                    self.sort.sorts.remove(pos);
                }
            }
        } else {
            self.sort
                .sorts
                .push((current_column, SortDirection::Ascending));
        }
        if self.sort.sorts.is_empty() {
            self.update_filter();
            return;
        }
        let sorts = self.sort.sorts.clone();
        let headers = self.headers.clone();
        self.view = match apply_sorts(self.view.clone(), &sorts, &headers) {
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

    pub fn clear_sorts(&mut self) {
        self.sort.sorts.clear();
        self.sort.error = None;
        self.update_filter();
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
        let label = if let Some(pos) = self.sort.sorts.iter().position(|(c, _)| *c == col_idx) {
            let dir = if matches!(self.sort.sorts[pos].1, SortDirection::Descending) {
                "▼"
            } else {
                "▲"
            };
            // ①–⑳ (U+2460–U+2473); fall back to plain number beyond 20
            let glyph = char::from_u32(0x2460 + pos as u32)
                .map_or_else(|| (pos + 1).to_string(), |c| c.to_string());
            format!("{} {}{}", base, glyph, dir)
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
        if self.groupby.keys.is_empty() || self.groupby.aggs.is_empty() {
            return;
        }

        // Stash the raw schema first so `run_groupby_agg` can resolve keys/aggs.
        self.groupby.saved_headers = self.headers.clone();
        self.groupby.saved_column_widths = self.column_widths.clone();

        // Probe the grouped schema by aggregating the full raw df. Committed
        // raw-column filters will be re-applied below via `update_filter`.
        let grouped_df = match self.run_groupby_agg(self.df.clone()) {
            Ok(df) => df,
            Err(_) => {
                self.groupby.saved_headers.clear();
                self.groupby.saved_column_widths.clear();
                return;
            }
        };

        self.headers = grouped_df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.column_widths = vec![config::DEFAULT_COLUMN_WIDTH; grouped_df.width()];

        // Drop any sort or in-progress filter: their indices referenced the raw
        // schema and would be meaningless under the grouped schema.
        self.sort.sorts.clear();
        self.sort.error = None;
        self.filter.query.clear();
        self.filter.col = None;
        self.filter.error = None;
        self.search.results = Vec::new();
        self.search.cursor = 0;
        self.groupby.active = true;
        self.state.select(Some(0));
        self.state.select_column(Some(0));

        // Rebuild view through the pipeline so raw-column filters reshape the
        // input before aggregation.
        self.update_filter();
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
        self.sort.sorts.clear();
        self.search.results = Vec::new();
        self.search.cursor = 0;

        // Preserve filters still tied to raw columns; drop the ones that
        // referenced aggregated columns that no longer exist.
        let raw_cols: HashSet<String> = self
            .df
            .get_column_names()
            .iter()
            .map(|s| s.to_string())
            .collect();
        self.filter
            .filters
            .retain(|(col, _)| raw_cols.contains(col));
        self.filter.query.clear();
        self.filter.col = None;
        self.filter.error = None;

        self.update_filter();
    }
}

#[cfg(test)]
#[path = "app_tests.rs"]
mod app_tests;
