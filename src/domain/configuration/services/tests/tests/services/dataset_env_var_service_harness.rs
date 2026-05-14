// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::CatalogBuilder;
pub use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_datasets::{
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarResolver,
    DatasetEnvVarsConfig,
    UpsertDatasetEnvVarStatus,
};
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Extended harness adding the dataset entry repo and mutation adapter config
/// on top of `BaseConfigurationServiceHarness`. Used for tests that exercise
/// `DatasetEnvVarMutationAdapterImpl` (lazy resource creation,
/// delete-last-entry).
#[oop::extend(BaseConfigurationServiceHarness, base)]
pub struct DatasetEnvVarServiceHarness {
    base: BaseConfigurationServiceHarness,
    catalog: dill::Catalog,
}

impl DatasetEnvVarServiceHarness {
    pub fn new() -> Self {
        let base = BaseConfigurationServiceHarness::new();

        let mut b = CatalogBuilder::new_chained(base.catalog());
        b.add::<InMemoryDatasetEntryRepository>()
            .add_value(DatasetEnvVarsConfig::sample());

        let catalog = b.build();

        Self { base, catalog }
    }

    pub fn mutation_adapter(&self) -> Arc<dyn DatasetEnvVarMutationAdapter> {
        self.catalog.get_one().unwrap()
    }

    pub fn resolver(&self) -> Arc<dyn DatasetEnvVarResolver> {
        self.catalog.get_one().unwrap()
    }

    pub async fn seed_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
    ) {
        use kamu_datasets::{DatasetEntry, DatasetEntryRepository};
        let repo = self
            .catalog
            .get_one::<dyn DatasetEntryRepository>()
            .unwrap();

        repo.save_dataset_entry(&DatasetEntry::new(
            dataset_id.clone(),
            account_id.clone(),
            odf::AccountName::new_unchecked("test-account"),
            odf::DatasetName::new_unchecked("test-dataset"),
            Utc::now(),
            odf::DatasetKind::Root,
        ))
        .await
        .unwrap();
    }

    pub fn assert_upsert_created(result: &kamu_datasets::DatasetEnvVarUpsertResult) {
        pretty_assertions::assert_eq!(result.status, UpsertDatasetEnvVarStatus::Created);
    }

    pub fn assert_upsert_updated(result: &kamu_datasets::DatasetEnvVarUpsertResult) {
        pretty_assertions::assert_eq!(result.status, UpsertDatasetEnvVarStatus::Updated);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
