// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use bon::bon;
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::*;
use kamu_core::auth::AlwaysHappyDatasetActionAuthorizer;
use kamu_core::{DidGeneratorDefault, RunInfoDir, TenancyConfig};
use kamu_datasets::*;
use kamu_datasets_inmem::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use messaging_outbox::*;
use tempfile::TempDir;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct BaseGQLDatasetHarness {
    _tempdir: TempDir,
    catalog: Catalog,
}

#[bon]
impl BaseGQLDatasetHarness {
    #[builder]
    pub fn new(tenancy_config: TenancyConfig) -> Self {
        let tempdir = tempfile::tempdir().unwrap();

        let datasets_dir = tempdir.path().join("datasets");
        std::fs::create_dir(&datasets_dir).unwrap();

        let run_info_dir = tempdir.path().join("run");
        std::fs::create_dir(&run_info_dir).unwrap();

        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_builder(
                OutboxImmediateImpl::builder().with_consumer_filter(ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<DidGeneratorDefault>()
            .add_value(tenancy_config)
            .add::<DatabaseTransactionRunner>()
            .add_builder(odf::dataset::DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
            .bind::<dyn odf::DatasetStorageUnit, odf::dataset::DatasetStorageUnitLocalFs>()
            .bind::<dyn odf::DatasetStorageUnitWriter, odf::dataset::DatasetStorageUnitLocalFs>()
            .add::<DatabaseBackedOdfDatasetLfsBuilderImpl>()
            .add::<CreateDatasetFromSnapshotUseCaseImpl>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<CreateDatasetUseCaseHelper>()
            .add::<ViewDatasetUseCaseImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<AlwaysHappyDatasetActionAuthorizer>()
            .add::<DatasetReferenceServiceImpl>()
            .add::<InMemoryDatasetReferenceRepository>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<DependencyGraphImmediateListener>()
            .add::<DatasetEntryServiceImpl>()
            .add::<InMemoryDatasetEntryRepository>()
            .add_value(RunInfoDir::new(run_info_dir));

            NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            register_message_dispatcher::<DatasetReferenceMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_REFERENCE_SERVICE,
            );

            register_message_dispatcher::<DatasetDependenciesMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
            );

            b.build()
        };

        Self {
            _tempdir: tempdir,
            catalog,
        }
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
