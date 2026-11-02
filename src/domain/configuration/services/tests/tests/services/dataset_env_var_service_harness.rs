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
use event_sourcing::Aggregate;
use kamu_accounts_services::AccountServiceImpl;
use kamu_configuration::{
    Secret,
    SecretSetResource,
    SecretSetSpecInput,
    Variable,
    VariableSetResource,
    VariableSetSpecInput,
};
use kamu_configuration_services::DatasetEnvVarMutationAdapterImpl;
pub use kamu_configuration_services::testing::BaseConfigurationServiceHarness;
use kamu_datasets::{
    CreateDatasetEnvVarError,
    DatasetEnvVar,
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarResolver,
    DatasetEnvVarSpecResolver,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    UpsertDatasetEnvVarError,
    UpsertDatasetEnvVarStatus,
};
use kamu_resources::{EventSourcedResource, ResourcePersistenceService};
use secrecy::SecretString;

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
        b.add::<AccountServiceImpl>();

        let catalog = b.build();

        Self { base, catalog }
    }

    pub fn mutation_adapter(&self) -> Arc<dyn DatasetEnvVarMutationAdapter> {
        self.catalog.get_one().unwrap()
    }

    /// The projection-backed resolver, as ingest resolves it.
    pub fn resolver(&self) -> Arc<dyn DatasetEnvVarResolver> {
        self.catalog.get_one().unwrap()
    }

    /// The spec-backed resolver, as the UI-facing GraphQL path resolves it.
    pub fn spec_resolver(&self) -> Arc<dyn DatasetEnvVarSpecResolver> {
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

    /// Seeds the dataset's legacy `VariableSet` the way the backfill
    /// migration wrote it: unvalidated, so keys such as `key-1` that today's
    /// rules reject are stored as they are.
    pub async fn seed_legacy_variable_set(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        variables: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) {
        let spec = VariableSetSpecInput::new(odf::metadata::config::VariableSetSpecInput {
            variables: odf::metadata::config::Variables {
                entries: variables
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            key.into(),
                            Variable {
                                value: value.into(),
                            },
                        )
                    })
                    .collect(),
            },
        });

        self.create_unvalidated::<VariableSetResource>(
            account,
            dataset_id,
            &DatasetEnvVarMutationAdapterImpl::legacy_variable_set_resource_name(dataset_id),
            spec,
        )
        .await;
    }

    /// [`Self::seed_legacy_variable_set`] for secrets, stored as plaintext.
    pub async fn seed_legacy_secret_set(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        secrets: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) {
        let spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
            secrets: odf::metadata::config::Secrets {
                entries: secrets
                    .into_iter()
                    .map(|(key, value)| {
                        (
                            key.into(),
                            Secret {
                                value: value.into(),
                                content_encoding: None,
                            },
                        )
                    })
                    .collect(),
            },
        });

        self.create_unvalidated::<SecretSetResource>(
            account,
            dataset_id,
            &DatasetEnvVarMutationAdapterImpl::legacy_secret_set_resource_name(dataset_id),
            spec,
        )
        .await;
    }

    async fn create_unvalidated<R>(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        spec: R::SpecInput,
    ) where
        R: EventSourcedResource + 'static,
    {
        let id = self.resource_repo().new_resource_id().await.unwrap();
        let headers = BaseConfigurationServiceHarness::make_targeting_headers_input(
            account,
            dataset_id,
            name,
            [],
        );

        let event = R::make_created_event(Utc::now(), id, headers, spec);
        let mut resource = R::from_aggregate(Aggregate::new(id, event).unwrap());

        let persistence_service: Arc<dyn ResourcePersistenceService<R>> =
            self.catalog.get_one().unwrap();
        persistence_service.create(&mut resource).await.unwrap();
    }

    pub async fn upsert_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &str,
    ) -> Result<DatasetEnvVarUpsertResult, UpsertDatasetEnvVarError> {
        self.mutation_adapter()
            .upsert_env_var(dataset_id, key, &DatasetEnvVarValue::Regular(value.into()))
            .await
    }

    pub async fn upsert_secret(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &str,
    ) -> Result<DatasetEnvVarUpsertResult, UpsertDatasetEnvVarError> {
        self.mutation_adapter()
            .upsert_env_var(
                dataset_id,
                key,
                &DatasetEnvVarValue::Secret(SecretString::from(value.to_string())),
            )
            .await
    }

    pub async fn create_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &str,
    ) -> Result<DatasetEnvVar, CreateDatasetEnvVarError> {
        self.mutation_adapter()
            .create_env_var(dataset_id, key, &DatasetEnvVarValue::Regular(value.into()))
            .await
    }

    /// Keys of the dataset's secrets, read from the stored specs
    pub async fn secret_keys(&self, dataset_id: &odf::DatasetID) -> Vec<String> {
        let mut keys: Vec<_> = self
            .spec_resolver()
            .resolve_effective_env_vars(dataset_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, env_var)| env_var.secret_nonce.is_some())
            .map(|(key, _)| key)
            .collect();
        keys.sort();
        keys
    }

    /// Plaintext values of the dataset's regular variables, read from the
    /// stored specs, by key
    pub async fn variable_values(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> std::collections::BTreeMap<String, String> {
        self.spec_resolver()
            .resolve_effective_env_vars(dataset_id)
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, env_var)| env_var.secret_nonce.is_none())
            .map(|(key, env_var)| (key, String::from_utf8(env_var.value).unwrap()))
            .collect()
    }

    pub fn assert_upsert_created(result: &kamu_datasets::DatasetEnvVarUpsertResult) {
        pretty_assertions::assert_eq!(result.status, UpsertDatasetEnvVarStatus::Created);
    }

    pub fn assert_upsert_updated(result: &kamu_datasets::DatasetEnvVarUpsertResult) {
        pretty_assertions::assert_eq!(result.status, UpsertDatasetEnvVarStatus::Updated);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
