// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_configuration::{
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    SecretSetProjectionRepository,
    SecretSetResource,
    SecretSetSpecInput,
    Variable,
    VariableSetProjectionRepository,
    VariableSetResource,
    VariableSetSpecInput,
};
use kamu_configuration_inmem::{
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_datasets::SecretsEncryptionConfig;
use kamu_resources::{
    ApplyResourceParams,
    ApplyResourceUseCase,
    DeclarativeResource,
    RequestResourcesDeletionUseCase,
    ResourceAggregateLoader,
    ResourceController,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceHeadersInput,
    ResourceID,
    ResourceLabelProjectionRepository,
    ResourcePhase,
    ResourceRepository,
    ResourceSchemaProvider,
    ResourceSnapshot,
    TypeUri,
};
use kamu_resources_services::testing::BaseResourceServiceHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Harness for configuration domain service tests. Chains configuration
/// repositories and services on top of `BaseResourceServiceHarness`.
/// Reusable by any crate that needs the resource + configuration service layer
/// without a GQL adapter.
#[oop::extend(BaseResourceServiceHarness, base)]
pub struct BaseConfigurationServiceHarness {
    base: BaseResourceServiceHarness,
    catalog: dill::Catalog,
}

impl BaseConfigurationServiceHarness {
    pub fn new() -> Self {
        let base = BaseResourceServiceHarness::new_with_additional_dependencies(|b| {
            kamu_datasets_inmem::register_dependencies(b);
            b.add_value(SecretsEncryptionConfig::sample())
                .add::<InMemoryVariableSetProjectionRepository>()
                .add::<InMemorySecretSetProjectionRepository>();

            crate::register_dependencies(b);
        });
        let catalog = base.catalog().clone();

        Self { base, catalog }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn variable_set_projection_repo(&self) -> Arc<dyn VariableSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn secret_set_projection_repo(&self) -> Arc<dyn SecretSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn apply_variable_use_case(&self) -> Arc<dyn ApplyResourceUseCase<VariableSetResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn apply_secret_use_case(&self) -> Arc<dyn ApplyResourceUseCase<SecretSetResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn variable_set_controller(&self) -> Arc<dyn ResourceController<VariableSetResource>> {
        self.catalog.get_one().unwrap()
    }

    pub fn secret_set_controller(&self) -> Arc<dyn ResourceController<SecretSetResource>> {
        self.catalog.get_one().unwrap()
    }

    pub async fn load_variable_set(&self, id: &ResourceID) -> VariableSetResource {
        let loader: Arc<dyn ResourceAggregateLoader<VariableSetResource>> =
            self.catalog.get_one().unwrap();
        loader.load(id).await.unwrap()
    }

    pub async fn load_secret_set(&self, id: &ResourceID) -> SecretSetResource {
        let loader: Arc<dyn ResourceAggregateLoader<SecretSetResource>> =
            self.catalog.get_one().unwrap();
        loader.load(id).await.unwrap()
    }

    pub fn resource_repo(&self) -> Arc<dyn ResourceRepository> {
        self.catalog.get_one().unwrap()
    }

    pub fn resource_label_projection_repo(&self) -> Arc<dyn ResourceLabelProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    /// IDs of `owner`'s `VariableSet` resources labelled as targeting
    /// `dataset_id`, oldest first — exactly what the resolver sees.
    pub async fn variable_sets_targeting(
        &self,
        owner: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
    ) -> Vec<ResourceID> {
        self.resources_targeting(owner, VariableSetResource::schema(), dataset_id)
            .await
    }

    /// IDs of `owner`'s `SecretSet` resources labelled as targeting
    /// `dataset_id`.
    pub async fn secret_sets_targeting(
        &self,
        owner: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
    ) -> Vec<ResourceID> {
        self.resources_targeting(owner, SecretSetResource::schema(), dataset_id)
            .await
    }

    async fn resources_targeting(
        &self,
        owner: &odf::ResourceOwnerHandle,
        schema: &TypeUri,
        dataset_id: &odf::DatasetID,
    ) -> Vec<ResourceID> {
        self.resource_repo()
            .find_resource_ids_by_schema_and_label(
                &owner.did,
                schema,
                RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
                &dataset_id.as_did_str().to_string(),
            )
            .await
            .unwrap()
    }

    /// Creates a `VariableSet` resource labelled as targeting `dataset_id`.
    pub async fn seed_variable_set_targeting(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        created_at: DateTime<Utc>,
    ) -> ResourceID {
        self.seed_resource_targeting(
            VariableSetResource::schema(),
            account,
            dataset_id,
            name,
            created_at,
        )
        .await
    }

    /// Creates a `SecretSet` resource labelled as targeting `dataset_id`.
    pub async fn seed_secret_set_targeting(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        created_at: DateTime<Utc>,
    ) -> ResourceID {
        self.seed_resource_targeting(
            SecretSetResource::schema(),
            account,
            dataset_id,
            name,
            created_at,
        )
        .await
    }

    /// Creates a `VariableSet` through the real apply path; reconciliation
    /// populates its projection entries before this returns.
    pub async fn apply_variable_set(
        &self,
        account: &odf::ResourceOwnerHandle,
        name: &str,
        variables: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        self.apply_variable_set_with_headers(
            BaseResourceServiceHarness::make_headers_input(account.clone(), name),
            variables,
        )
        .await
    }

    /// [`Self::apply_variable_set`] for secrets, which arrive as plaintext
    /// and are sanitized to `jwe` like any normally-authored set.
    pub async fn apply_secret_set(
        &self,
        account: &odf::ResourceOwnerHandle,
        name: &str,
        secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        self.apply_secret_set_with_headers(
            BaseResourceServiceHarness::make_headers_input(account.clone(), name),
            secrets,
        )
        .await
    }

    /// Seeds a labelled `VariableSet` through the real apply path, so it gets
    /// an event-sourced aggregate. Required by tests that mutate the result;
    /// [`Self::seed_variable_set_targeting`] only writes a snapshot.
    pub async fn apply_variable_set_targeting(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        variables: impl IntoIterator<Item = (&'static str, &'static str)>,
        extra_labels: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        self.apply_variable_set_with_headers(
            Self::make_targeting_headers_input(account, dataset_id, name, extra_labels),
            variables,
        )
        .await
    }

    /// [`Self::apply_variable_set_targeting`] for secrets.
    pub async fn apply_secret_set_targeting(
        &self,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
        extra_labels: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        self.apply_secret_set_with_headers(
            Self::make_targeting_headers_input(account, dataset_id, name, extra_labels),
            secrets,
        )
        .await
    }

    async fn apply_variable_set_with_headers(
        &self,
        headers: ResourceHeadersInput,
        variables: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        let spec = VariableSetSpecInput::new(odf::metadata::config::VariableSetSpecInput {
            variables: odf::metadata::config::Variables {
                entries: variables
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            Variable {
                                value: value.to_string(),
                            },
                        )
                    })
                    .collect(),
            },
        });

        let decision = self
            .apply_variable_use_case()
            .apply(ApplyResourceParams {
                id: None,
                headers,
                spec,
            })
            .await
            .unwrap();

        decision.expect_applied().id
    }

    async fn apply_secret_set_with_headers(
        &self,
        headers: ResourceHeadersInput,
        secrets: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceID {
        let spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
            secrets: odf::metadata::config::Secrets {
                entries: secrets
                    .into_iter()
                    .map(|(name, value)| {
                        (
                            name.to_string(),
                            odf::metadata::config::Secret {
                                value: value.to_string(),
                                content_encoding: None,
                            },
                        )
                    })
                    .collect(),
            },
        });

        let decision = self
            .apply_secret_use_case()
            .apply(ApplyResourceParams {
                id: None,
                headers,
                spec,
            })
            .await
            .unwrap();

        decision.expect_applied().id
    }

    /// Requests deletion of `account_id`'s `VariableSet`s. The immediate
    /// outbox delivers `DeletionRequested` right away, so the deletion is
    /// completed, cleanup included, before this returns.
    pub async fn request_variable_sets_deletion(
        &self,
        account_id: &odf::AccountID,
        ids: Vec<ResourceID>,
    ) {
        let use_case: Arc<dyn RequestResourcesDeletionUseCase<VariableSetResource>> =
            self.catalog.get_one().unwrap();
        use_case.execute(account_id.clone(), ids).await.unwrap();
    }

    /// [`Self::request_variable_sets_deletion`] for `SecretSet`s.
    pub async fn request_secret_sets_deletion(
        &self,
        account_id: &odf::AccountID,
        ids: Vec<ResourceID>,
    ) {
        let use_case: Arc<dyn RequestResourcesDeletionUseCase<SecretSetResource>> =
            self.catalog.get_one().unwrap();
        use_case.execute(account_id.clone(), ids).await.unwrap();
    }

    /// Runs the `VariableSet` controller's cleanup directly, outside the
    /// deletion flow.
    pub async fn cleanup_variable_set(&self, id: &ResourceID) {
        let resource = self.load_variable_set(id).await;
        self.variable_set_controller()
            .cleanup(&resource)
            .await
            .unwrap();
    }

    /// Runs the `SecretSet` controller's cleanup directly, outside the
    /// deletion flow.
    pub async fn cleanup_secret_set(&self, id: &ResourceID) {
        let resource = self.load_secret_set(id).await;
        self.secret_set_controller()
            .cleanup(&resource)
            .await
            .unwrap();
    }

    pub async fn variable_set_phase(&self, id: &ResourceID) -> ResourcePhase {
        self.load_variable_set(id).await.status().phase
    }

    pub async fn secret_set_phase(&self, id: &ResourceID) -> ResourcePhase {
        self.load_secret_set(id).await.status().phase
    }

    pub async fn variable_set_projection_entry_count(&self, id: &ResourceID) -> usize {
        self.variable_set_projection_repo()
            .get_latest_entries(id)
            .await
            .unwrap()
            .len()
    }

    pub async fn secret_set_projection_entry_count(&self, id: &ResourceID) -> usize {
        self.secret_set_projection_repo()
            .get_latest_entries(id)
            .await
            .unwrap()
            .len()
    }

    fn make_targeting_headers_input(
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        extra_labels: impl IntoIterator<Item = (&'static str, &'static str)>,
    ) -> ResourceHeadersInput {
        let mut headers = BaseResourceServiceHarness::make_headers_input(account.clone(), name);

        let mut entries: std::collections::BTreeMap<_, _> = [(
            RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI
                .parse()
                .unwrap(),
            serde_json::Value::String(dataset_id.as_did_str().to_string()),
        )]
        .into_iter()
        .collect();

        for (key, value) in extra_labels {
            entries.insert(
                key.parse().unwrap(),
                serde_json::Value::String(value.to_string()),
            );
        }

        headers.labels = Some(kamu_resources::ResourceLabels { entries });
        headers
    }

    /// Creates a resource carrying the `legacy-config-target-dataset` label,
    /// writing both the snapshot and its label projection row the way
    /// `sync_snapshots` does in production.
    ///
    /// `created_at` is explicit because it decides resolution precedence.
    async fn seed_resource_targeting(
        &self,
        schema: &TypeUri,
        account: &odf::ResourceOwnerHandle,
        dataset_id: &odf::DatasetID,
        name: &str,
        created_at: DateTime<Utc>,
    ) -> ResourceID {
        let repo = self.resource_repo();
        let id = repo.new_resource_id().await.unwrap();

        let label_key = RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI;
        let label_value = dataset_id.as_did_str().to_string();

        let mut headers = ResourceHeaders::simple(created_at, id, account.clone(), name);
        headers.created_at = created_at;
        headers.updated_at = created_at;
        headers.labels.entries = [(
            label_key.parse().unwrap(),
            serde_json::Value::String(label_value.clone()),
        )]
        .into_iter()
        .collect();

        repo.create_resource(&ResourceSnapshot {
            id,
            schema: schema.clone(),
            headers,
            spec: serde_json::json!({}),
            status: None,
            last_event_id: None,
        })
        .await
        .unwrap();

        self.resource_label_projection_repo()
            .replace_entries(&id, &[(label_key.to_string(), label_value)])
            .await
            .unwrap();

        id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
