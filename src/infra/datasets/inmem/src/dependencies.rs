// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Registers every in-memory dataset repository with the state it shares
/// across catalogs and transactions
pub fn register_dependencies(b: &mut CatalogBuilder) {
    b.add::<InMemoryDatasetEntryState>();
    b.add::<InMemoryDatasetEntryRepository>();

    b.add::<InMemoryDatasetReferenceState>();
    b.add::<InMemoryDatasetReferenceRepository>();

    b.add::<InMemoryDatasetDependencyState>();
    b.add::<InMemoryDatasetDependencyRepository>();

    b.add::<InMemoryDatasetKeyBlockState>();
    b.add::<InMemoryDatasetKeyBlockRepository>();

    b.add::<InMemoryDatasetDataBlockState>();
    b.add::<InMemoryDatasetDataBlockRepository>();

    b.add::<InMemoryDatasetStatisticsState>();
    b.add::<InMemoryDatasetStatisticsRepository>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
