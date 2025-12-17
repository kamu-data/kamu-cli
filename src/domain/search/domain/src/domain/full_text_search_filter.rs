// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::FullTextSearchFieldPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FullTextSearchFilterExpr {
    Field {
        field: FullTextSearchFieldPath,
        op: FullTextSearchFilterOp,
    },

    And(Vec<FullTextSearchFilterExpr>),

    Or(Vec<FullTextSearchFilterExpr>),

    Not(Box<FullTextSearchFilterExpr>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FullTextSearchFilterExpr {
    pub fn and_clauses(clauses: Vec<FullTextSearchFilterExpr>) -> Self {
        assert!(!clauses.is_empty());
        if clauses.len() == 1 {
            clauses.into_iter().next().unwrap()
        } else {
            FullTextSearchFilterExpr::And(clauses)
        }
    }

    pub fn or_clauses(clauses: Vec<FullTextSearchFilterExpr>) -> Self {
        assert!(!clauses.is_empty());
        if clauses.len() == 1 {
            clauses.into_iter().next().unwrap()
        } else {
            FullTextSearchFilterExpr::Or(clauses)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// AND: variable number of arguments
#[macro_export]
macro_rules! filter_and {
    // Zero arguments → explicit compile error
    () => {
        compile_error!("filter_and!() requires at least one argument")
    };

    // 1 arg -> just return the expr
    ($single:expr) => {
        $single
    };

    // 2+ args -> And(vec![...])
    ($first:expr, $($rest:expr),+ $(,)?) => {
        $crate::FullTextSearchFilterExpr::And(vec![$first, $($rest), +])
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// OR: variable number of arguments
#[macro_export]
macro_rules! filter_or {
    // Zero arguments → explicit compile error
    () => {
        compile_error!("filter_or!() requires at least one argument")
    };

    ($single:expr) => {
        $single
    };

    ($first:expr, $($rest:expr),+ $(,)?) => {
        $crate::FullTextSearchFilterExpr::Or(vec![$first, $($rest),+])
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! filter_not {
    ($expr:expr) => {
        $crate::FullTextSearchFilterExpr::Not(Box::new($expr))
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FullTextSearchFilterOp {
    Eq(serde_json::Value),
    Ne(serde_json::Value),
    Lt(serde_json::Value),
    Lte(serde_json::Value),
    Gt(serde_json::Value),
    Gte(serde_json::Value),
    In(Vec<serde_json::Value>),
    Prefix(String),
    // TODO: Add more operators as needed
}

#[inline]
pub fn field_eq_str(field: FullTextSearchFieldPath, value: &str) -> FullTextSearchFilterExpr {
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::Eq(serde_json::json!(value)),
    }
}

#[inline]
pub fn field_lte_num(field: FullTextSearchFieldPath, value: i64) -> FullTextSearchFilterExpr {
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::Lte(serde_json::json!(value)),
    }
}

#[inline]
pub fn field_in_str<S>(
    field: FullTextSearchFieldPath,
    values: impl IntoIterator<Item = S>,
) -> FullTextSearchFilterExpr
where
    S: Into<String>,
{
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::In(
            values
                .into_iter()
                .map(|v| serde_json::Value::String(v.into()))
                .collect(),
        ),
    }
}

#[inline]
pub fn field_prefix(field: FullTextSearchFieldPath, prefix: &str) -> FullTextSearchFilterExpr {
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::Prefix(prefix.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
