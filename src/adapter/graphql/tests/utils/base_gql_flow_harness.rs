// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::MetadataQueryServiceImpl;
use kamu_datasets::*;
use kamu_flow_system_inmem::*;
use odf::metadata::testing::MetadataFactory;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
pub struct BaseGQLFlowHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    pub catalog_anonymous: dill::Catalog,
    pub catalog_authorized: dill::Catalog,
}

impl BaseGQLFlowHarness {
    pub async fn new(base_gql_harness: BaseGQLDatasetHarness, catalog: dill::Catalog) -> Self {
        let (catalog_anonymous, catalog_authorized) =
            authentication_catalogs(&catalog, PredefinedAccountOpts::default()).await;

        Self {
            base_gql_harness,
            catalog_anonymous,
            catalog_authorized,
        }
    }

    pub fn make_base_gql_flow_catalog(base_gql_harness: &BaseGQLDatasetHarness) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

        b.add::<MetadataQueryServiceImpl>()
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryFlowProcessState>();

        b.build()
    }

    pub async fn create_root_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .push_event(MetadataFactory::set_polling_source().build())
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_root_dataset_no_source(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .kind(odf::DatasetKind::Root)
                    .name(dataset_alias)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
        inputs: &[odf::DatasetAlias],
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(odf::DatasetKind::Derivative)
                    .push_event(
                        MetadataFactory::set_transform()
                            .inputs_from_refs(inputs)
                            .build(),
                    )
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }

    pub async fn create_derived_dataset_no_transform(
        &self,
        dataset_alias: odf::DatasetAlias,
    ) -> CreateDatasetResult {
        let create_dataset_from_snapshot = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetFromSnapshotUseCase>()
            .unwrap();

        create_dataset_from_snapshot
            .execute(
                MetadataFactory::dataset_snapshot()
                    .name(dataset_alias)
                    .kind(odf::DatasetKind::Derivative)
                    .build(),
                Default::default(),
            )
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
