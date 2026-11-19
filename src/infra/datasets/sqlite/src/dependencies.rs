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

/// Registers every SQLite dataset repository
pub fn register_dependencies(b: &mut CatalogBuilder) {
    b.add::<SqliteDatasetEntryRepository>();
    b.add::<SqliteDatasetDependencyRepository>();
    b.add::<SqliteDatasetReferenceRepository>();
    b.add::<SqliteDatasetStatisticsRepository>();
    b.add::<SqliteDatasetKeyBlockRepository>();
    b.add::<SqliteDatasetDataBlockRepository>();
    b.add::<SqliteDatasetBlockQueryRepository>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
