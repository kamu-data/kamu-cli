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
