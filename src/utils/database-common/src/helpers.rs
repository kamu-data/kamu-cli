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

    pretty_assertions::assert_eq!("", f(0, NonZeroUsize::new(0).unwrap()));
    pretty_assertions::assert_eq!("$0", f(1, NonZeroUsize::new(0).unwrap()));
    pretty_assertions::assert_eq!("$0,$1,$2", f(3, NonZeroUsize::new(0).unwrap()));
    pretty_assertions::assert_eq!("$3,$4", f(2, NonZeroUsize::new(3).unwrap()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test]
fn test_sqlite_generate_placeholders_tuple_list_2() {
    use sqlite_generate_placeholders_tuple_list_2 as f;

    pretty_assertions::assert_eq!("", f(0, NonZeroUsize::new(0).unwrap()));
    pretty_assertions::assert_eq!("($0,$1)", f(1, NonZeroUsize::new(0).unwrap()));
    pretty_assertions::assert_eq!(
        "($0,$1),($2,$3),($4,$5)",
        f(3, NonZeroUsize::new(0).unwrap())
    );
    pretty_assertions::assert_eq!("($3,$4),($5,$6)", f(2, NonZeroUsize::new(3).unwrap()));
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

    pretty_assertions::assert_eq!("NULL", f(0));
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
