// Tests for src/app.rs. Loaded via `#[path = "app_tests.rs"] mod app_tests;` in app.rs so
// that `use super::*;` below gives every sub-module access to app's private items
// (FilterQuery, parse_operator, etc.) without needing to make them pub.
use super::*;

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
        app.filter.filters = vec![("name".to_string(), "Bob".to_string())];
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
        app.filter.filters = vec![("name".to_string(), "zzznomatch".to_string())];
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
        app.filter.filters = vec![("val".to_string(), "zzznomatch".to_string())];
        app.update_filter();
        // Should return default stats without panicking
        let stats = app.compute_stats(0);
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn test_filter_to_zero_rows() {
        let mut app = make_app();
        app.filter.filters = vec![("name".to_string(), "zzznomatch".to_string())];
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

    #[test]
    fn test_sort_preserved_after_filter() {
        // Sort descending on "age" first, then apply a filter — result must remain sorted.
        let mut app = make_app();
        app.state.select_column(Some(1)); // age column
        app.sort_by_column(); // ascending
        app.sort_by_column(); // descending: 35, 30, 25

        // Filter to rows where age >= 25 (all rows, but exercises the code path)
        app.filter.filters = vec![("age".to_string(), ">= 25".to_string())];
        app.update_filter();

        // Still sorted descending
        let ages: Vec<i64> = app
            .view
            .column("age")
            .unwrap()
            .as_series()
            .unwrap()
            .i64()
            .unwrap()
            .into_iter()
            .flatten()
            .collect();
        assert_eq!(ages, vec![35, 30, 25]);
    }

    #[test]
    fn test_sort_three_state_cycle() {
        let mut app = make_app();
        app.state.select_column(Some(0));

        // Not in list → Ascending
        app.sort_by_column();
        assert_eq!(app.sort.sorts.len(), 1);
        assert!(matches!(app.sort.sorts[0].1, SortDirection::Ascending));

        // Ascending → Descending
        app.sort_by_column();
        assert_eq!(app.sort.sorts.len(), 1);
        assert!(matches!(app.sort.sorts[0].1, SortDirection::Descending));

        // Descending → removed; view must be restored to original order
        app.sort_by_column();
        assert!(app.sort.sorts.is_empty());
        assert_eq!(get_str(&app, "name", 0), "Alice");
        assert_eq!(get_str(&app, "name", 1), "Bob");
        assert_eq!(get_str(&app, "name", 2), "Charlie");
    }

    #[test]
    fn test_clear_sorts_restores_original_order() {
        let mut app = make_app();
        app.state.select_column(Some(1)); // age column: 30, 25, 35
        app.sort_by_column(); // ascending: 25, 30, 35 → Bob, Alice, Charlie
        assert_eq!(get_str(&app, "name", 0), "Bob");

        app.clear_sorts();
        assert!(app.sort.sorts.is_empty());
        // Original order: Alice, Bob, Charlie
        assert_eq!(get_str(&app, "name", 0), "Alice");
        assert_eq!(get_str(&app, "name", 1), "Bob");
        assert_eq!(get_str(&app, "name", 2), "Charlie");
    }

    #[test]
    fn test_two_column_sort() {
        let df = df! {
            "dept" => ["eng", "hr", "eng"],
            "sal"  => [200i64, 150, 100],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());

        // Primary sort: dept ascending
        app.state.select_column(Some(0));
        app.sort_by_column();
        // Secondary sort: sal ascending
        app.state.select_column(Some(1));
        app.sort_by_column();

        assert_eq!(app.sort.sorts.len(), 2);

        // dept: eng, eng, hr — primary sort ascending
        let dept_0 = get_str(&app, "dept", 0);
        let dept_1 = get_str(&app, "dept", 1);
        let dept_2 = get_str(&app, "dept", 2);
        assert_eq!(dept_0, "eng");
        assert_eq!(dept_1, "eng");
        assert_eq!(dept_2, "hr");

        // Within eng group, sal should be ascending (100 before 200)
        let sal_0 = app
            .view
            .column("sal")
            .unwrap()
            .as_series()
            .unwrap()
            .i64()
            .unwrap()
            .get(0)
            .unwrap();
        let sal_1 = app
            .view
            .column("sal")
            .unwrap()
            .as_series()
            .unwrap()
            .i64()
            .unwrap()
            .get(1)
            .unwrap();
        assert!(sal_0 < sal_1);
    }

    #[test]
    fn test_multi_sort_preserved_after_filter() {
        let df = df! {
            "dept" => ["eng", "hr", "eng", "hr"],
            "sal"  => [200i64, 150, 100, 300],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());

        // Sort by dept asc (primary), sal asc (secondary)
        app.state.select_column(Some(0));
        app.sort_by_column();
        app.state.select_column(Some(1));
        app.sort_by_column();

        // Filter: sal > 100 removes the eng/100 row
        app.filter.filters = vec![("sal".to_string(), "> 100".to_string())];
        app.update_filter();

        assert_eq!(app.view.height(), 3);
        assert_eq!(app.sort.sorts.len(), 2);
        // eng comes first (primary sort)
        assert_eq!(get_str(&app, "dept", 0), "eng");
    }

    #[test]
    fn test_apply_groupby_clears_sorts() {
        let df = df! {
            "dept" => ["eng", "eng", "hr"],
            "sal"  => [100i64, 200, 150],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());

        app.state.select_column(Some(1));
        app.sort_by_column();
        assert!(!app.sort.sorts.is_empty());

        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(1));
        app.cycle_groupby_agg();
        app.apply_groupby();

        assert!(app.sort.sorts.is_empty());
    }
}

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

    #[test]
    fn test_sort_cycle_preserves_groupby() {
        // Regression: pressing 's' three times in groupby view must not dissolve
        // the aggregated view back into the raw dataframe.
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(1));
        app.cycle_groupby_agg();
        app.apply_groupby();
        assert_eq!(app.headers, vec!["dept", "sal_sum"]);
        assert_eq!(app.view.height(), 2);

        app.state.select_column(Some(1));
        app.sort_by_column(); // asc
        app.sort_by_column(); // desc
        app.sort_by_column(); // removes sort → update_filter path

        assert_eq!(app.view.height(), 2, "groupby view was lost");
        assert_eq!(app.view.get_column_names(), vec!["dept", "sal_sum"]);
        assert!(app.groupby.active);
    }

    #[test]
    fn test_clear_sorts_preserves_groupby() {
        // `S` (clear_sorts) must also keep the grouped view intact.
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(1));
        app.cycle_groupby_agg();
        app.apply_groupby();

        app.state.select_column(Some(1));
        app.sort_by_column();
        app.clear_sorts();

        assert_eq!(app.view.height(), 2);
        assert_eq!(app.view.get_column_names(), vec!["dept", "sal_sum"]);
        assert!(app.groupby.active);
    }

    #[test]
    fn test_filter_applies_pre_aggregation() {
        // A raw-column filter should reshape the rows that feed the aggregation,
        // not the aggregated output.
        let df = df! {
            "dept"   => ["eng", "eng", "eng", "hr", "hr"],
            "region" => ["N",   "N",   "S",   "N",  "S"],
            "sal"    => [100i64, 200, 300, 150, 250],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());

        // Filter to North only, then groupby dept with sum(sal)
        app.filter.filters = vec![("region".to_string(), "= N".to_string())];
        app.update_filter();

        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(2));
        app.cycle_groupby_agg();
        app.apply_groupby();

        // eng=N has 100+200=300; hr=N has 150. Without pre-agg filter we'd see
        // eng=600 and hr=400 instead.
        let dept_col = app
            .view
            .column("dept")
            .unwrap()
            .as_series()
            .unwrap()
            .cast(&DataType::String)
            .unwrap();
        let sums: HashMap<String, i64> = (0..app.view.height())
            .map(|i| {
                (
                    dept_col.str().unwrap().get(i).unwrap_or("").to_string(),
                    app.view
                        .column("sal_sum")
                        .unwrap()
                        .as_series()
                        .unwrap()
                        .i64()
                        .unwrap()
                        .get(i)
                        .unwrap(),
                )
            })
            .collect();
        assert_eq!(sums.get("eng"), Some(&300));
        assert_eq!(sums.get("hr"), Some(&150));
    }

    #[test]
    fn test_filter_on_aggregated_column() {
        // A filter added while in groupby view targets the aggregated schema
        // and narrows the post-agg rows.
        let mut app = make_app();
        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(1));
        app.cycle_groupby_agg();
        app.apply_groupby();
        // eng=300, hr=150 — filter sal_sum > 200 keeps only eng.
        app.filter
            .filters
            .push(("sal_sum".to_string(), "> 200".to_string()));
        app.update_filter();
        assert_eq!(app.view.height(), 1);
        let dept0 = app.view.column("dept").unwrap().get(0).unwrap().to_string();
        assert!(dept0.contains("eng"), "expected 'eng', got {dept0}");
    }

    #[test]
    fn test_clear_groupby_keeps_raw_filters() {
        // Filters on raw columns should survive clear_groupby; filters on
        // aggregated columns should be dropped.
        let df = df! {
            "dept"   => ["eng", "eng", "hr"],
            "region" => ["N",   "S",   "N"],
            "sal"    => [100i64, 200, 150],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());

        app.filter.filters = vec![("region".to_string(), "= N".to_string())];
        app.update_filter();

        app.state.select_column(Some(0));
        app.toggle_groupby_key();
        app.state.select_column(Some(2));
        app.cycle_groupby_agg();
        app.apply_groupby();

        app.filter
            .filters
            .push(("sal_sum".to_string(), "> 0".to_string()));
        app.update_filter();

        app.clear_groupby();
        // Raw-column filter kept, aggregated-column filter dropped.
        assert_eq!(
            app.filter.filters,
            vec![("region".to_string(), "= N".to_string())]
        );
        assert_eq!(app.view.height(), 2); // two North rows in raw data
    }
}

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
        let col_name = app.headers[col_idx].clone();
        app.filter.filters = vec![(col_name, query.to_string())];
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

    fn toggle_y_col(app: &mut App, col: usize) {
        if let Some(pos) = app.plot.y_cols.iter().position(|&c| c == col) {
            app.plot.y_cols.remove(pos);
        } else {
            app.plot.y_cols.push(col);
        }
    }

    #[test]
    fn test_plot_pick_y_toggle_adds_column() {
        let df = df! { "a" => [1i32], "b" => [2i32] }.unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.plot.y_cols.clear();
        toggle_y_col(&mut app, 0);
        assert_eq!(app.plot.y_cols, vec![0]);
    }

    #[test]
    fn test_plot_pick_y_toggle_removes_column() {
        let df = df! { "a" => [1i32], "b" => [2i32] }.unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.plot.y_cols = vec![0];
        toggle_y_col(&mut app, 0);
        assert!(app.plot.y_cols.is_empty());
    }

    #[test]
    fn test_plot_pick_y_toggle_twice_restores_state() {
        let df = df! { "a" => [1i32], "b" => [2i32] }.unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.plot.y_cols.clear();
        toggle_y_col(&mut app, 1);
        toggle_y_col(&mut app, 1);
        assert!(
            app.plot.y_cols.is_empty(),
            "double-toggle should return to empty"
        );
    }

    #[test]
    fn test_plot_pick_y_toggle_multiple_columns() {
        let df = df! { "a" => [1i32], "b" => [2i32], "c" => [3i32] }.unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.plot.y_cols.clear();
        toggle_y_col(&mut app, 0);
        toggle_y_col(&mut app, 2);
        assert_eq!(app.plot.y_cols, vec![0, 2]);
        // removing one should not affect the other
        toggle_y_col(&mut app, 0);
        assert_eq!(app.plot.y_cols, vec![2]);
    }
}

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

mod incomplete_operator_tests {
    use super::*;

    #[test]
    fn test_bare_gt_is_incomplete() {
        assert!(FilterQuery::parse(">").is_none());
    }

    #[test]
    fn test_bare_gte_is_incomplete() {
        assert!(FilterQuery::parse(">=").is_none());
    }

    #[test]
    fn test_bare_neq_is_incomplete() {
        assert!(FilterQuery::parse("!=").is_none());
    }

    #[test]
    fn test_operator_with_value_is_not_incomplete() {
        assert!(FilterQuery::parse("> 5").is_some());
    }

    #[test]
    fn test_plain_text_is_not_incomplete() {
        assert!(FilterQuery::parse("hello").is_some());
    }

    #[test]
    fn test_empty_string_is_not_incomplete() {
        assert!(FilterQuery::parse("").is_some());
    }
}

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
        let fq = FilterQuery::parse("> 20").unwrap();
        assert!(fq.validate("age", &make_df()).is_none());
    }

    #[test]
    fn test_numeric_op_on_string_col_returns_error() {
        let fq = FilterQuery::parse("> 5").unwrap();
        let err = fq.validate("name", &make_df());
        assert!(err.is_some());
        assert!(err.unwrap().contains("numeric"));
    }

    #[test]
    fn test_numeric_op_with_non_numeric_value_returns_error() {
        let fq = FilterQuery::parse("> abc").unwrap();
        let err = fq.validate("age", &make_df());
        assert!(err.is_some());
        assert!(err.unwrap().contains("number"));
    }

    #[test]
    fn test_eq_op_on_string_col_is_valid() {
        // = and != are string-compatible, so no error
        let fq = FilterQuery::parse("= Alice").unwrap();
        assert!(fq.validate("name", &make_df()).is_none());
    }

    #[test]
    fn test_substring_query_is_valid() {
        let fq = FilterQuery::parse("Ali").unwrap();
        assert!(fq.validate("name", &make_df()).is_none());
    }
}

mod filter_query_tests {
    use super::*;

    #[test]
    fn test_filter_query_parse_returns_none_for_incomplete_operator() {
        assert!(FilterQuery::parse(">").is_none());
        assert!(FilterQuery::parse(">=").is_none());
        assert!(FilterQuery::parse("!=").is_none());
    }

    #[test]
    fn test_filter_query_parse_returns_some_for_complete_query() {
        assert!(FilterQuery::parse("> 5").is_some());
        assert!(FilterQuery::parse("alice").is_some());
        assert!(FilterQuery::parse("= (null)").is_some());
    }
}

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
        app.filter.filters = vec![
            ("dept".to_string(), "eng".to_string()),
            ("sal".to_string(), "> 150".to_string()),
        ];
        app.update_filter();
        assert_eq!(app.view.height(), 1);
    }

    #[test]
    fn test_duplicate_filter_not_stacked() {
        let mut app = make_app();
        app.filter.filters = vec![("dept".to_string(), "eng".to_string())];
        app.update_filter();
        let height_first = app.view.height();

        // Simulate pressing Enter with the same filter again
        let col_name = "dept".to_string();
        let query = "eng".to_string();
        let already_exists = app
            .filter
            .filters
            .iter()
            .any(|(c, q)| c == &col_name && q == &query);
        if !already_exists {
            app.filter.filters.push((col_name, query));
            app.update_filter();
        }

        assert_eq!(app.view.height(), height_first);
        assert_eq!(app.filter.filters.len(), 1);
    }

    #[test]
    fn test_range_filter_two_ops_same_column() {
        let mut app = make_app();
        // sal >= 100 AND sal <= 150 → 100, 150 rows
        app.filter.filters = vec![
            ("sal".to_string(), ">= 100".to_string()),
            ("sal".to_string(), "<= 150".to_string()),
        ];
        app.update_filter();
        assert_eq!(app.view.height(), 2);
    }

    #[test]
    fn test_filter_error_set_for_numeric_op_on_string_col() {
        let mut app = make_app();
        app.state.select_column(Some(0)); // dept (string)
        app.filter.query = "> 5".to_string();
        app.update_filter();
        assert!(app.filter.error.is_some());
    }
}

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

    fn make_columns_app() -> App {
        let df = df! {
            "first_name" => ["Alice", "Bob"],
            "last_name"  => ["Smith", "Jones"],
            "age"        => [30i64, 25],
            "city"       => ["NY", "LA"],
        }
        .unwrap();
        let mut app = App::new(df, "test.csv".to_string());
        app.build_columns_profile();
        app
    }

    #[test]
    fn test_filter_columns_profile_empty_query_shows_all() {
        let mut app = make_columns_app();
        app.columns_view.query.clear();
        app.filter_columns_profile();
        assert_eq!(app.columns_view.filtered, vec![0, 1, 2, 3]);
        assert_eq!(app.columns_view.state.selected(), Some(0));
    }

    #[test]
    fn test_filter_columns_profile_substring_match() {
        let mut app = make_columns_app();
        app.columns_view.query = "name".to_string();
        app.filter_columns_profile();
        // first_name (0), last_name (1)
        assert_eq!(app.columns_view.filtered, vec![0, 1]);
    }

    #[test]
    fn test_filter_columns_profile_case_insensitive() {
        let mut app = make_columns_app();
        app.columns_view.query = "CITY".to_string();
        app.filter_columns_profile();
        assert_eq!(app.columns_view.filtered, vec![3]);
    }

    #[test]
    fn test_filter_columns_profile_no_match_clears_selection() {
        let mut app = make_columns_app();
        app.columns_view.query = "xyz".to_string();
        app.filter_columns_profile();
        assert!(app.columns_view.filtered.is_empty());
        assert_eq!(app.columns_view.state.selected(), None);
    }

    #[test]
    fn test_build_columns_profile_resets_query_and_filtered() {
        let mut app = make_columns_app();
        app.columns_view.query = "stale".to_string();
        app.columns_view.filtered = vec![];
        app.columns_view.searching = true;
        app.build_columns_profile();
        assert!(app.columns_view.query.is_empty());
        assert!(!app.columns_view.searching);
        assert_eq!(
            app.columns_view.filtered.len(),
            app.columns_view.profile.len()
        );
        assert_eq!(app.columns_view.state.selected(), Some(0));
    }
}
