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
    In(Vec<serde_json::Value>),
    // TODO: Add more operators as needed
}

impl FullTextSearchFilterOp {
    pub fn eq_str(value: &str) -> Self {
        FullTextSearchFilterOp::Eq(serde_json::json!(value))
    }

    pub fn in_str(values: &[&str]) -> Self {
        FullTextSearchFilterOp::In(values.iter().map(|v| serde_json::json!(v)).collect())
    }
}

#[inline]
pub fn field_eq(field: FullTextSearchFieldPath, value: &str) -> FullTextSearchFilterExpr {
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::eq_str(value),
    }
}

#[inline]
pub fn field_in(field: FullTextSearchFieldPath, values: &[&str]) -> FullTextSearchFilterExpr {
    FullTextSearchFilterExpr::Field {
        field,
        op: FullTextSearchFilterOp::in_str(values),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
