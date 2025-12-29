// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::utils::CreateDatasetUseCaseHelper;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(b: &mut dill::CatalogBuilder, needs_indexing: bool) {
    b.add::<DatasetFullTextSearchSchemaProvider>();
    b.add::<DatasetFullTextSearchUpdater>();

    if needs_indexing {
        b.add::<DatasetEntryIndexer>();
        b.add::<DatasetReferenceIndexer>();
        b.add::<DatasetStatisticsIndexer>();
        b.add::<DependencyGraphIndexer>();
        b.add::<DatasetBlockIndexer>();
    }

    b.add::<AppendDatasetMetadataBatchUseCaseImpl>();
    b.add::<CommitDatasetEventUseCaseImpl>();
    b.add::<CreateDatasetFromSnapshotUseCaseImpl>();
    b.add::<CreateDatasetUseCaseImpl>();
    b.add::<DeleteDatasetUseCaseImpl>();
    b.add::<GetDatasetDownstreamDependenciesUseCaseImpl>();
    b.add::<GetDatasetUpstreamDependenciesUseCaseImpl>();
    b.add::<RenameDatasetUseCaseImpl>();

    b.add::<FindCollectionEntriesUseCaseImpl>();
    b.add::<UpdateCollectionEntriesUseCaseImpl>();
    b.add::<ViewCollectionEntriesUseCaseImpl>();

    b.add::<FindVersionedFileVersionUseCaseImpl>();
    b.add::<ViewVersionedFileHistoryUseCaseImpl>();
    b.add::<UpdateVersionedFileUseCaseImpl>();

    b.add::<CreateDatasetUseCaseHelper>();

    b.add::<DatasetEntryServiceImpl>();
    b.add::<DependencyGraphServiceImpl>();
    b.add::<DatasetReferenceServiceImpl>();
    b.add::<DatasetStatisticsServiceImpl>();
    b.add::<DatasetIncrementQueryServiceImpl>();
    b.add::<AccountQuotaCheckerStorageImpl>();

    b.add::<DatasetAliasUpdateHandler>();
    b.add::<DatasetBlockUpdateHandler>();
    b.add::<DatasetStatisticsUpdateHandler>();
    b.add::<DependencyGraphImmediateListener>();

    b.add::<DatasetAccountLifecycleHandler>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
