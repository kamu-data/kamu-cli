// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::SearchFieldPath;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum SearchFilterExpr {
    Field {
        field: SearchFieldPath,
        op: SearchFilterOp,
    },

    And(Vec<SearchFilterExpr>),

    Or(Vec<SearchFilterExpr>),

    Not(Box<SearchFilterExpr>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SearchFilterExpr {
    pub fn and_clauses(clauses: Vec<SearchFilterExpr>) -> Self {
        assert!(!clauses.is_empty());
        if clauses.len() == 1 {
            clauses.into_iter().next().unwrap()
        } else {
            SearchFilterExpr::And(clauses)
        }
    }

    pub fn or_clauses(clauses: Vec<SearchFilterExpr>) -> Self {
        assert!(!clauses.is_empty());
        if clauses.len() == 1 {
            clauses.into_iter().next().unwrap()
        } else {
            SearchFilterExpr::Or(clauses)
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
        $crate::SearchFilterExpr::And(vec![$first, $($rest), +])
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
        $crate::SearchFilterExpr::Or(vec![$first, $($rest),+])
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! filter_not {
    ($expr:expr) => {
        $crate::SearchFilterExpr::Not(Box::new($expr))
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum SearchFilterOp {
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
pub fn field_eq_str(field: SearchFieldPath, value: &str) -> SearchFilterExpr {
    SearchFilterExpr::Field {
        field,
        op: SearchFilterOp::Eq(serde_json::json!(value)),
    }
}

#[inline]
pub fn field_lte_num(field: SearchFieldPath, value: i64) -> SearchFilterExpr {
    SearchFilterExpr::Field {
        field,
        op: SearchFilterOp::Lte(serde_json::json!(value)),
    }
}

#[inline]
pub fn field_in_str<S>(
    field: SearchFieldPath,
    values: impl IntoIterator<Item = S>,
) -> SearchFilterExpr
where
    S: Into<String>,
{
    SearchFilterExpr::Field {
        field,
        op: SearchFilterOp::In(
            values
                .into_iter()
                .map(|v| serde_json::Value::String(v.into()))
                .collect(),
        ),
    }
}

#[inline]
pub fn field_prefix(field: SearchFieldPath, prefix: &str) -> SearchFilterExpr {
    SearchFilterExpr::Field {
        field,
        op: SearchFilterOp::Prefix(prefix.to_string()),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
