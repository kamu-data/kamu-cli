// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use crypto_utils::{AesGcmEncryptor, Encryptor};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts::AccountService;
use kamu_configuration::{
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    Secret,
    SecretSetProjectionRepository,
    SecretSetResource,
    SecretSetSpecInput,
    Variable,
    VariableSetProjectionRepository,
    VariableSetResource,
    VariableSetSpec,
    VariableSetSpecInput,
};
use kamu_datasets::{
    DatasetEntry,
    DatasetEntryRepository,
    DatasetEnvVar,
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEntryError,
    SecretsEncryptionConfig,
    UpsertDatasetEnvVarStatus,
};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    GenericResourceQueryService,
    ResourceCrudDispatcher,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceHeadersInput,
    ResourceID,
    ResourceName,
    ResourceSchemaProvider,
    ResourceSpecFromInput,
    TypeUri,
};
use kamu_resources_services::ResourceDispatcherFactory;
use secrecy::ExposeSecret;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarMutationAdapter)]
pub struct DatasetEnvVarMutationAdapterImpl {
    dispatcher_factory: Arc<ResourceDispatcherFactory>,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    account_service: Arc<dyn AccountService>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    variable_set_projection_repo: Arc<dyn VariableSetProjectionRepository>,
    secret_set_projection_repo: Arc<dyn SecretSetProjectionRepository>,
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarMutationAdapter for DatasetEnvVarMutationAdapterImpl {
    async fn upsert_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &DatasetEnvVarValue,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let dataset_entry = self.get_dataset_entry(dataset_id).await?;

        match value {
            DatasetEnvVarValue::Regular(plaintext) => {
                self.upsert_variable(
                    dataset_id,
                    key,
                    plaintext,
                    &dataset_entry.owner_id,
                    &dataset_entry.owner_name,
                )
                .await
            }
            DatasetEnvVarValue::Secret(secret) => {
                self.upsert_secret(
                    dataset_id,
                    key,
                    secret.expose_secret(),
                    &dataset_entry.owner_id,
                    &dataset_entry.owner_name,
                )
                .await
            }
        }
    }

    async fn delete_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_env_var_key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        // Resolved once and threaded down: every lookup below is scoped to the
        // dataset owner, and re-fetching the entry per lookup would query the
        // same row up to three times in one call.
        let owner_id = self.get_dataset_entry(dataset_id).await?.owner_id;

        // Try deleting as a variable
        let was_variable = self
            .delete_if_variable(dataset_id, &owner_id, dataset_env_var_key)
            .await?;
        if was_variable {
            return Ok(());
        }

        // Maybe it's a secret then?
        let was_secret = self
            .delete_if_secret(dataset_id, &owner_id, dataset_env_var_key)
            .await?;
        if was_secret {
            return Ok(());
        }

        Err(DeleteDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: dataset_env_var_key.to_string(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetEnvVarMutationAdapterImpl {
    pub fn legacy_variable_set_resource_name(dataset_id: &odf::DatasetID) -> ResourceName {
        ResourceName::new_unchecked(&format!("legacy-vars-{}", dataset_id.as_multibase()))
    }

    pub fn legacy_secret_set_resource_name(dataset_id: &odf::DatasetID) -> ResourceName {
        ResourceName::new_unchecked(&format!("legacy-secrets-{}", dataset_id.as_multibase()))
    }

    fn get_dispatcher(
        &self,
        schema: &str,
    ) -> Result<Arc<dyn ResourceCrudDispatcher>, InternalError> {
        // Only the two built-in legacy schemas are ever passed here, so an
        // unsupported schema is an internal wiring error, not a user error.
        self.dispatcher_factory
            .crud_dispatcher(schema)
            .map_err(ErrorIntoInternal::int_err)
    }

    async fn apply_and_handle_rejection<E>(
        &self,
        dispatcher: &Arc<dyn ResourceCrudDispatcher>,
        request: ResourceCrudDispatcherApplyRequest,
        resource_type_name: &str,
    ) -> Result<ResourceID, E>
    where
        E: From<InternalError>,
    {
        let apply_decision = dispatcher.apply(request).await.int_err().map_err(E::from)?;

        match apply_decision {
            ApplyManifestApplicationDecision::Applied(result) => Ok(result.resource.headers.id),
            ApplyManifestApplicationDecision::Rejected(rejection) => Err(E::from(
                format!("{resource_type_name} apply rejected: {}", rejection.message).int_err(),
            )),
        }
    }

    async fn upsert_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        plaintext: &str,
        account_did: &odf::AccountID,
        account_name: &odf::AccountName,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = Self::legacy_variable_set_resource_name(dataset_id);
        let (existing_id, mut variables) = self
            .load_existing_variable_spec(account_did, &resource_name)
            .await?;

        let exists_as_variable = variables.contains_key(key);

        // If key is new to variables, it may be a class conversion from secret →
        // variable
        let existed_as_secret = if !exists_as_variable {
            self.delete_if_secret(dataset_id, account_did, key).await?
        } else {
            false
        };

        let is_new_key = !exists_as_variable && !existed_as_secret;

        variables.insert(
            key.to_string(),
            Variable {
                value: plaintext.to_string(),
            },
        );

        let new_spec = serde_json::to_value(VariableSetSpecInput::new(
            odf::metadata::config::VariableSetSpecInput {
                variables: odf::metadata::config::Variables { entries: variables },
            },
        ))
        .int_err()?;
        let headers = self
            .make_headers(
                account_did.clone(),
                account_name.clone(),
                resource_name,
                dataset_id,
            )
            .await?;

        let dispatcher = self.get_dispatcher(VariableSetResource::SCHEMA_STR)?;

        self.apply_and_handle_rejection::<InternalError>(
            &dispatcher,
            ResourceCrudDispatcherApplyRequest {
                id: existing_id,
                headers,
                spec: new_spec,
            },
            "VariableSet",
        )
        .await?;

        let status = if is_new_key {
            UpsertDatasetEnvVarStatus::Created
        } else {
            UpsertDatasetEnvVarStatus::Updated
        };

        Ok(DatasetEnvVarUpsertResult {
            dataset_env_var: DatasetEnvVar {
                key: key.to_string(),
                value: plaintext.as_bytes().to_vec(),
                secret_nonce: None,
                created_at: Utc::now(),
                dataset_id: dataset_id.clone(),
            },
            status,
        })
    }

    async fn upsert_secret(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        plaintext: &str,
        account_did: &odf::AccountID,
        account_name: &odf::AccountName,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = Self::legacy_secret_set_resource_name(dataset_id);

        let (existing_id, mut secrets) = self
            .load_existing_secret_spec_decrypted(account_did, &resource_name)
            .await?;

        let exists_as_secret = secrets.contains_key(key);

        // If key is new to secrets, it may be a class conversion from variable → secret
        let existed_as_variable = if !exists_as_secret {
            self.delete_if_variable(dataset_id, account_did, key)
                .await?
        } else {
            false
        };
        let is_new_key = !exists_as_secret && !existed_as_variable;

        secrets.insert(
            key.to_string(),
            Secret {
                value: plaintext.to_string(),
                content_encoding: None,
            },
        );

        let new_spec = serde_json::to_value(SecretSetSpecInput::new(
            odf::metadata::config::SecretSetSpecInput {
                secrets: odf::metadata::config::Secrets { entries: secrets },
            },
        ))
        .int_err()?;
        let headers = self
            .make_headers(
                account_did.clone(),
                account_name.clone(),
                resource_name,
                dataset_id,
            )
            .await?;

        let dispatcher = self.get_dispatcher(SecretSetResource::SCHEMA_STR)?;

        self.apply_and_handle_rejection::<InternalError>(
            &dispatcher,
            ResourceCrudDispatcherApplyRequest {
                id: existing_id,
                headers,
                spec: new_spec,
            },
            "SecretSet",
        )
        .await?;

        let encryption_key = self
            .secrets_encryption_config
            .encryption_key
            .as_deref()
            .unwrap_or("");
        let encryptor = AesGcmEncryptor::try_new(encryption_key).int_err()?;
        let (encrypted_value, nonce) = encryptor.encrypt_bytes(plaintext.as_bytes()).int_err()?;

        let status = if is_new_key {
            UpsertDatasetEnvVarStatus::Created
        } else {
            UpsertDatasetEnvVarStatus::Updated
        };

        Ok(DatasetEnvVarUpsertResult {
            dataset_env_var: DatasetEnvVar {
                key: key.to_string(),
                value: encrypted_value,
                secret_nonce: Some(nonce),
                created_at: Utc::now(),
                dataset_id: dataset_id.clone(),
            },
            status,
        })
    }

    async fn delete_variable(
        &self,
        resource_id: ResourceID,
        key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_id(&resource_id)
            .await
            .int_err()?
            .ok_or_else(|| {
                DeleteDatasetEnvVarError::Internal(
                    format!("VariableSet resource {resource_id} not found in snapshot store")
                        .int_err(),
                )
            })?;

        let mut spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        spec.variables.entries.remove(key);

        let dispatcher = self
            .get_dispatcher(VariableSetResource::SCHEMA_STR)
            .map_err(DeleteDatasetEnvVarError::Internal)?;

        if spec.variables.entries.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.headers.account.did.clone(),
                    ids: vec![resource_id],
                })
                .await
                .int_err()?;
        } else {
            let headers = ResourceHeadersInput {
                id: Some(snapshot.headers.id),
                account: Some(odf::metadata::auth::AccountRef {
                    id: Some(snapshot.headers.account.id),
                    did: Some(snapshot.headers.account.did),
                    name: Some(snapshot.headers.account.name),
                }),
                name: snapshot.headers.name,
                labels: Some(snapshot.headers.labels),
                annotations: Some(snapshot.headers.annotations),
            };
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    id: Some(resource_id),
                    headers,
                    spec: serde_json::to_value(spec.into_input()).int_err()?,
                },
                "VariableSet",
            )
            .await
            .map_err(DeleteDatasetEnvVarError::Internal)?;
        }

        Ok(())
    }

    async fn delete_secret(
        &self,
        resource_id: ResourceID,
        key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_id(&resource_id)
            .await
            .int_err()?
            .ok_or_else(|| {
                DeleteDatasetEnvVarError::Internal(
                    format!("SecretSet resource {resource_id} not found in snapshot store")
                        .int_err(),
                )
            })?;

        let mut decrypted = self.decrypt_secret_entries(&resource_id).await.int_err()?;
        decrypted.remove(key);

        let dispatcher = self
            .get_dispatcher(SecretSetResource::SCHEMA_STR)
            .map_err(DeleteDatasetEnvVarError::Internal)?;

        if decrypted.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.headers.account.did.clone(),
                    ids: vec![resource_id],
                })
                .await
                .int_err()?;
        } else {
            let new_spec = SecretSetSpecInput::new(odf::metadata::config::SecretSetSpecInput {
                secrets: odf::metadata::config::Secrets {
                    entries: decrypted
                        .into_iter()
                        .map(|(k, v)| {
                            (
                                k,
                                Secret {
                                    value: v,
                                    content_encoding: None,
                                },
                            )
                        })
                        .collect(),
                },
            });
            let headers = ResourceHeadersInput {
                id: Some(snapshot.headers.id),
                account: Some(odf::metadata::auth::AccountRef {
                    id: Some(snapshot.headers.account.id),
                    did: Some(snapshot.headers.account.did),
                    name: Some(snapshot.headers.account.name),
                }),
                name: snapshot.headers.name,
                labels: Some(snapshot.headers.labels),
                annotations: Some(snapshot.headers.annotations),
            };
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    id: Some(resource_id),
                    headers,
                    spec: serde_json::to_value(new_spec).int_err()?,
                },
                "SecretSet",
            )
            .await
            .map_err(DeleteDatasetEnvVarError::Internal)?;
        }

        Ok(())
    }

    /// Returns true if `key` existed as a secret for `dataset_id` and was
    /// deleted. Used when converting a key from secret to regular variable.
    async fn delete_if_secret(
        &self,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
        key: &str,
    ) -> Result<bool, InternalError> {
        match self.find_secret(dataset_id, owner_id, key).await? {
            Some((resource_id, key)) => self
                .delete_secret(resource_id, &key)
                .await
                .map_err(|e| match e {
                    DeleteDatasetEnvVarError::NotFound(e) => e.int_err(),
                    DeleteDatasetEnvVarError::Internal(e) => e,
                })
                .map(|_| true),
            None => Ok(false),
        }
    }

    /// Returns true if `key` existed as a variable for `dataset_id` and was
    /// deleted. Used when converting a key from regular variable to secret.
    async fn delete_if_variable(
        &self,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
        key: &str,
    ) -> Result<bool, InternalError> {
        match self.find_variable(dataset_id, owner_id, key).await? {
            Some((resource_id, key)) => self
                .delete_variable(resource_id, &key)
                .await
                .map_err(|e| match e {
                    DeleteDatasetEnvVarError::NotFound(e) => e.int_err(),
                    DeleteDatasetEnvVarError::Internal(e) => e,
                })
                .map(|_| true),
            None => Ok(false),
        }
    }

    async fn find_secret(
        &self,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
        key: &str,
    ) -> Result<Option<(ResourceID, String)>, InternalError> {
        let Some(resource_id) = self
            .find_legacy_resource_id(
                owner_id,
                SecretSetResource::schema(),
                &Self::legacy_secret_set_resource_name(dataset_id),
            )
            .await?
        else {
            return Ok(None);
        };

        let entries = self
            .secret_set_projection_repo
            .get_latest_entries(&resource_id)
            .await?;

        if entries.iter().any(|e| e.key == key) {
            return Ok(Some((resource_id, key.to_string())));
        }

        Ok(None)
    }

    async fn find_variable(
        &self,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
        key: &str,
    ) -> Result<Option<(ResourceID, String)>, InternalError> {
        let Some(resource_id) = self
            .find_legacy_resource_id(
                owner_id,
                VariableSetResource::schema(),
                &Self::legacy_variable_set_resource_name(dataset_id),
            )
            .await?
        else {
            return Ok(None);
        };

        let entries = self
            .variable_set_projection_repo
            .get_latest_entries(&resource_id)
            .await?;

        if entries.iter().any(|e| e.key == key) {
            return Ok(Some((resource_id, key.to_string())));
        }

        Ok(None)
    }

    /// Loads the dataset entry, whose `owner_id` scopes every resource lookup
    /// this adapter makes. Resolved once per public entry point and threaded
    /// down, rather than re-fetched by each lookup.
    async fn get_dataset_entry(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetEntry, InternalError> {
        self.dataset_entry_repository
            .get_dataset_entry(dataset_id)
            .await
            .map_err(|e| match e {
                GetDatasetEntryError::NotFound(nf) => nf.int_err(),
                GetDatasetEntryError::Internal(e) => e,
            })
    }

    /// Locates the single auto-managed legacy resource for a dataset, among
    /// `owner_id`'s resources.
    ///
    /// Deliberately by well-known name rather than by the target label: the
    /// legacy read-modify-write path owns exactly one resource per dataset per
    /// kind, whereas the label may legitimately match several user-authored
    /// sets that this path must not touch.
    async fn find_legacy_resource_id(
        &self,
        owner_id: &odf::AccountID,
        schema: &TypeUri,
        resource_name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        self.generic_resource_query_service
            .find_resource_id_by_name(owner_id, schema, resource_name)
            .await
    }

    // Returns (existing_id, variables_map, existing_entry_id_for_key)
    async fn load_existing_variable_spec(
        &self,
        account_id: &odf::AccountID,
        resource_name: &ResourceName,
    ) -> Result<(Option<ResourceID>, BTreeMap<String, Variable>), InternalError> {
        let id = self
            .generic_resource_query_service
            .find_resource_id_by_name(account_id, VariableSetResource::schema(), resource_name)
            .await?;

        let Some(id) = id else {
            return Ok((None, BTreeMap::new()));
        };

        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_id(&id)
            .await?
            .ok_or_else(|| format!("VariableSet {id} missing snapshot").int_err())?;

        let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        Ok((Some(id), spec.into_dto().variables.entries))
    }

    // Returns (existing_id, decrypted_secrets_map)
    async fn load_existing_secret_spec_decrypted(
        &self,
        account_id: &odf::AccountID,
        resource_name: &ResourceName,
    ) -> Result<(Option<ResourceID>, BTreeMap<String, Secret>), InternalError> {
        let id = self
            .generic_resource_query_service
            .find_resource_id_by_name(account_id, SecretSetResource::schema(), resource_name)
            .await?;

        let Some(id) = id else {
            return Ok((None, BTreeMap::new()));
        };

        let decrypted = self.decrypt_secret_entries(&id).await?;

        let secrets = decrypted
            .into_iter()
            .map(|(k, v)| {
                (
                    k,
                    Secret {
                        value: v,
                        content_encoding: None,
                    },
                )
            })
            .collect();

        Ok((Some(id), secrets))
    }

    async fn decrypt_secret_entries(
        &self,
        resource_id: &ResourceID,
    ) -> Result<BTreeMap<String, String>, InternalError> {
        let encryption_key = self
            .secrets_encryption_config
            .encryption_key
            .as_deref()
            .unwrap_or("");
        let encryptor = AesGcmEncryptor::try_new(encryption_key).int_err()?;

        let entries = self
            .secret_set_projection_repo
            .get_latest_entries(resource_id)
            .await?;

        let mut decrypted = BTreeMap::new();
        for entry in entries {
            let plaintext = encryptor
                .decrypt_bytes(&entry.value, &entry.secret_nonce)
                .int_err()?;
            let plaintext = String::from_utf8(plaintext).int_err()?;
            decrypted.insert(entry.key, plaintext);
        }
        Ok(decrypted)
    }

    /// Builds headers for an auto-managed legacy resource, stamped with the
    /// `legacy-config-target-dataset` label that associates it with its
    /// dataset. The label is what the resolver reads; without it the resource
    /// exists but never resolves.
    async fn make_headers(
        &self,
        account_did: odf::AccountID,
        account_name: odf::AccountName,
        resource_name: ResourceName,
        dataset_id: &odf::DatasetID,
    ) -> Result<ResourceHeadersInput, InternalError> {
        let account = self.account_service.get_account_by_id(&account_did).await;
        let account_resource_id = match account {
            Ok(account) => account.resource_id,
            Err(e) => return Err(e.int_err()),
        };

        Ok(ResourceHeadersInput {
            id: None,
            account: Some(odf::metadata::auth::AccountRef {
                id: Some(account_resource_id),
                did: Some(account_did),
                name: Some(account_name),
            }),
            name: resource_name,
            labels: Some(kamu_resources::ResourceLabels {
                entries: [(
                    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI
                        .parse()
                        .int_err()?,
                    serde_json::Value::String(dataset_id.as_did_str().to_string()),
                )]
                .into_iter()
                .collect(),
            }),
            annotations: Some(kamu_resources::ResourceAnnotations {
                entries: BTreeMap::new(),
            }),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
