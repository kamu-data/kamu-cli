// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{
    ResourceListColumnValueView,
    ResourcePresentationDefinition,
    ResourceSnapshot,
    TypeUri,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourcePresentationDispatcher: Send + Sync {
    fn schema(&self) -> &'static TypeUri;

    fn presentation(&self) -> ResourcePresentationDefinition;

    /// Computes the typed list columns for one stored resource.
    ///
    /// Takes a snapshot rather than a typed `ResourceState` so that a listing
    /// spanning several types can render columns for all of them from the
    /// results of a *single* scoped query. Rendering through each type's own
    /// paginated query cannot produce correct global pagination across types:
    /// page 2 of a merged result is not page 2 of each type.
    ///
    /// The caller is expected to have matched `snapshot.schema` to this
    /// dispatcher; a snapshot of another type yields an error rather than
    /// wrong columns.
    fn list_column_values_for_snapshot(
        &self,
        snapshot: &ResourceSnapshot,
    ) -> Result<Vec<ResourceListColumnValueView>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
