// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

use chrono::Utc;
use dill::CatalogBuilder;
use event_sourcing::Aggregate;
use internal_error::InternalError;
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
    GetDatasetEnvVarError,
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

    /// An adapter whose key lookups are each followed by one write of another
    /// request, `racing_write`, before the lookup answers: the window between
    /// a create's duplicate check and its write
    pub fn mutation_adapter_with_racing_write(
        &self,
        racing_write: (&str, DatasetEnvVarValue),
    ) -> Arc<dyn DatasetEnvVarMutationAdapter> {
        let (key, value) = racing_write;
        let racing_spec_resolver = RacingSpecResolver {
            inner: self.spec_resolver(),
            other_request_adapter: self.mutation_adapter(),
            racing_write: Mutex::new(Some((key.to_string(), value))),
        };

        Arc::new(DatasetEnvVarMutationAdapterImpl::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            Arc::new(racing_spec_resolver),
            self.catalog.get_one().unwrap(),
        ))
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
    pub async fn variable_values(&self, dataset_id: &odf::DatasetID) -> BTreeMap<String, String> {
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

/// Answers a key lookup, then runs one write of another request through the
/// regular adapter, before returning the now stale answer
struct RacingSpecResolver {
    inner: Arc<dyn DatasetEnvVarSpecResolver>,
    other_request_adapter: Arc<dyn DatasetEnvVarMutationAdapter>,
    racing_write: Mutex<Option<(String, DatasetEnvVarValue)>>,
}

#[async_trait::async_trait]
impl DatasetEnvVarSpecResolver for RacingSpecResolver {
    async fn resolve_effective_env_vars(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<HashMap<String, DatasetEnvVar>, InternalError> {
        self.inner.resolve_effective_env_vars(dataset_id).await
    }

    async fn get_env_var_by_entry_key(
        &self,
        dataset_id: &odf::DatasetID,
        entry_key: &str,
    ) -> Result<DatasetEnvVar, GetDatasetEnvVarError> {
        let answer = self
            .inner
            .get_env_var_by_entry_key(dataset_id, entry_key)
            .await;

        let racing_write = self.racing_write.lock().unwrap().take();
        if let Some((key, value)) = racing_write {
            self.other_request_adapter
                .upsert_env_var(dataset_id, &key, &value)
                .await
                .unwrap();
        }

        answer
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
