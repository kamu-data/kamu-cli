// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::TypeRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// An authored label filter, resolved by the facade before repository use.
pub type ResourceLabelFilterInput = odf::metadata::resource::LabelFilter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A label filter whose keys and string values have already been resolved.
/// Repositories consume this directly and do not validate schemas.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum ResolvedResourceLabelFilter {
    /// Matches everything.
    #[default]
    True,
    Eq {
        key: TypeRef,
        value: String,
    },
    And(Vec<ResolvedResourceLabelFilter>),
    Not(Box<ResolvedResourceLabelFilter>),
    Or(Vec<ResolvedResourceLabelFilter>),
}

impl ResolvedResourceLabelFilter {
    /// Whether this filter can be skipped when building a repository query.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::True => true,
            Self::And(children) | Self::Or(children) if children.is_empty() => true,
            _ => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A parsed, not-yet-resolved label filter expression.
/// Plain keys are equality leaves; `$not`/`$or` are recognized operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceLabelFilterExpr {
    /// Matches everything.
    True,
    Eq {
        key: String,
        value: serde_json::Value,
    },
    /// The implicit conjunction of a filter object's entries.
    And(Vec<ResourceLabelFilterExpr>),
    /// Negates the nested filter object.
    Not(Box<ResourceLabelFilterExpr>),
    /// Disjunction over the `$or` array's filter objects.
    Or(Vec<ResourceLabelFilterExpr>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ResourceLabelFilterExprParseError {
    #[error("'$not' must be an object, got: {0}")]
    NotExpectsObject(String),

    #[error("'$or' must be an array of objects, got: {0}")]
    OrExpectsArrayOfObjects(String),

    #[error("unknown filter operator key '{0}'")]
    UnknownOperator(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
