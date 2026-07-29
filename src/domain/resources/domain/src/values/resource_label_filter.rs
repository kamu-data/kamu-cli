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

/// An authored, not-yet-resolved label filter: raw string keys paired with
/// arbitrary JSON values, exactly as they arrive from a caller (CLI, GraphQL).
/// Resolved into a [`ResolvedResourceLabelFilter`] by the facade before it
/// reaches the repository layer.
pub type ResourceLabelFilterInput = odf::metadata::resource::LabelFilter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A single-type label filter that has already been resolved: every key is
/// canonical (short names replaced with their schema URI, unresolved short
/// names kept as free-form) and every value is a plain string equality
/// predicate. Repositories consume this directly and never resolve aliases
/// or validate schemas themselves.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResolvedResourceLabelFilter {
    pub entries: Vec<(TypeRef, String)>,
}

impl ResolvedResourceLabelFilter {
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A parsed (not yet resolved) label filter expression, mirroring the shape
/// the ODF `LabelFilter` JSON Schema anticipates: plain keys are equality
/// leaves, implicitly `ANDed` at the top level; `$not`/`$or` are reserved
/// combinator keys for future boolean expressions.
///
/// Only [`ResourceLabelFilterExpr::Eq`] is evaluated today —
/// [`ResourceLabelFilterExpr::Not`]/[`ResourceLabelFilterExpr::Or`] parse
/// successfully (so their shape is recognized rather than misclassified as an
/// invalid key or an unsupported value) but the resolver rejects them as
/// not-yet-supported before any resolution is attempted.
///
/// Parsed from raw entries by
/// [`ResourceLabelFilterExprParser`](crate::ResourceLabelFilterExprParser).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceLabelFilterExpr {
    Eq {
        key: String,
        value: serde_json::Value,
    },
    /// Negates the conjunction of the nested entries — `{"$not": {"a": "x",
    /// "b": "y"}}` negates `a=x AND b=y`, and the nested object may itself
    /// contain `$not`/`$or`, same as any other filter object.
    Not(Vec<ResourceLabelFilterExpr>),
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
