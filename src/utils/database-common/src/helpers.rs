// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SQLite
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Utility to generate the placeholder list. Helpful when using dynamic SQL
/// generation.
///
/// # Examples
/// ```
/// // Output for `arguments_count`=3 & `index_offset`=0
/// "$0,$1,$2"
///
/// // Output for `arguments_count`=2 & `index_offset`=3
/// "$3,$4"
/// ```
pub fn sqlite_generate_placeholders_list(
    arguments_count: usize,
    index_offset: NonZeroUsize,
) -> String {
    let index_offset = index_offset.get();
    (0..arguments_count)
        .map(|i| format!("${}", i + index_offset))
        .intersperse(",".to_string())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn sqlite_generate_placeholders_tuple_list_2(
    tuples_count: usize,
    index_offset: NonZeroUsize,
) -> String {
    let index_offset = index_offset.get();
    (0..tuples_count)
        .map(|i| {
            // i | idxs
            // 1 | 1, 2
            // 2 | 3, 4
            // 3 | 5, 6
            // ...
            let first_idx = i * 2 + index_offset;
            let second_idx = i * 2 + 1 + index_offset;

            format!("(${first_idx},${second_idx})")
        })
        .intersperse(",".to_string())
        .collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sqlite_generate_placeholders_list() {
    use sqlite_generate_placeholders_list as f;

    pretty_assertions::assert_eq!("", f(0, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!("$1", f(1, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!("$1,$2,$3", f(3, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!("$3,$4", f(2, NonZeroUsize::new(3).unwrap()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sqlite_generate_placeholders_tuple_list_2() {
    use sqlite_generate_placeholders_tuple_list_2 as f;

    pretty_assertions::assert_eq!("", f(0, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!("($1,$2)", f(1, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!(
        "($1,$2),($3,$4),($5,$6)",
        f(3, NonZeroUsize::new(1).unwrap())
    );
    pretty_assertions::assert_eq!("($3,$4),($5,$6)", f(2, NonZeroUsize::new(3).unwrap()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// LIKE pattern helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Escapes special SQL `LIKE` characters (`_` and `\`) while preserving `%` as
/// a caller-controlled wildcard.
///
/// Use this when the caller intentionally supplies `%` wildcards in the
/// pattern (e.g. `"prefix%"` for a prefix search).
///
/// The resulting string should be paired with `ESCAPE '\'` in the SQL query.
pub fn sql_like_escape_pattern(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    for ch in pattern.chars() {
        match ch {
            '%' => out.push('%'),
            '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

/// Escapes all special SQL `LIKE` characters (`%`, `_`, and `\`) so the input
/// is treated as a literal search term.
///
/// Use this for substring searches where the surrounding `%` wildcards are
/// added by the SQL itself (e.g. `LIKE '%' || $1 || '%'`).
///
/// The resulting string should be paired with `ESCAPE '\'` in the SQL query.
pub fn sql_like_escape_literal(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len());
    for ch in pattern.chars() {
        match ch {
            '%' | '_' | '\\' => {
                out.push('\\');
                out.push(ch);
            }
            _ => out.push(ch),
        }
    }
    out
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sql_like_escape_pattern() {
    use sql_like_escape_pattern as f;

    pretty_assertions::assert_eq!("foo%", f("foo%"));
    pretty_assertions::assert_eq!("foo\\_bar", f("foo_bar"));
    pretty_assertions::assert_eq!("foo\\\\bar", f("foo\\bar"));
    pretty_assertions::assert_eq!("foo\\_bar%", f("foo_bar%"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sql_like_escape_literal() {
    use sql_like_escape_literal as f;

    pretty_assertions::assert_eq!("foo", f("foo"));
    pretty_assertions::assert_eq!("foo\\_bar", f("foo_bar"));
    pretty_assertions::assert_eq!("foo\\%bar", f("foo%bar"));
    pretty_assertions::assert_eq!("foo\\\\bar", f("foo\\bar"));
    pretty_assertions::assert_eq!("foo\\_\\%\\\\bar", f("foo_%\\bar"));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MySQL
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn mysql_generate_placeholders_list(arguments_count: usize) -> String {
    if arguments_count == 0 {
        // MySQL does not consider the "IN ()" syntax correct,
        // so we add a subquery that has nothing rows in the result:
        //
        // ```sql
        // SELECT *
        // FROM table
        // WHERE id IN (SELECT NULL WHERE FALSE);
        // -- output: empty (nothing included)
        //
        // SELECT *
        // FROM table
        // WHERE id NOT IN (SELECT NULL WHERE FALSE);
        // -- output: all rows (nothing excluded)
        // ```
        return "(SELECT NULL WHERE FALSE)".to_string();
    }

    (0..arguments_count).map(|_| "?").intersperse(",").collect()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_mysql_generate_placeholders_list() {
    use mysql_generate_placeholders_list as f;

    pretty_assertions::assert_eq!("(SELECT NULL WHERE FALSE)", f(0));
    pretty_assertions::assert_eq!("?", f(1));
    pretty_assertions::assert_eq!("?,?", f(2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// PostgreSQL
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn postgres_generate_placeholders_tuple_list_2(
    tuples_count: usize,
    index_offset: NonZeroUsize,
) -> String {
    // Postgres also uses $N (e.g. $1) as placeholders, so we reuse the function
    // https://www.postgresql.org/docs/current/sql-prepare.html
    sqlite_generate_placeholders_tuple_list_2(tuples_count, index_offset)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_postgres_generate_placeholders_tuple_list_2() {
    use postgres_generate_placeholders_tuple_list_2 as f;

    pretty_assertions::assert_eq!("", f(0, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!("($1,$2)", f(1, NonZeroUsize::new(1).unwrap()));
    pretty_assertions::assert_eq!(
        "($1,$2),($3,$4),($5,$6)",
        f(3, NonZeroUsize::new(1).unwrap())
    );
    pretty_assertions::assert_eq!("($3,$4),($5,$6)", f(2, NonZeroUsize::new(3).unwrap()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
