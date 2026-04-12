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

    #[test]
    fn test_sort_preserved_after_filter() {
        // Sort descending on "age" first, then apply a filter — result must remain sorted.
        let mut app = make_app();
        app.state.select_column(Some(1)); // age column
        app.sort_by_column(); // ascending
        app.sort_by_column(); // descending: 35, 30, 25

        // Filter to rows where age >= 25 (all rows, but exercises the code path)
        app.filter.filters = vec![(1, ">= 25".to_string())];
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
}
