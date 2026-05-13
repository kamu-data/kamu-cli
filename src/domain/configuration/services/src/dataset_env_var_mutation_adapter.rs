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
use kamu_configuration::{
    DatasetSecretSetBindingRepository,
    DatasetVariableSetBindingRepository,
    SecretSetProjectionRepository,
    SecretSetResource,
    SecretSetSpec,
    SecretSpec,
    VariableSetProjectionRepository,
    VariableSetResource,
    VariableSetSpec,
    VariableSpec,
};
use kamu_datasets::{
    DatasetEntryRepository,
    DatasetEnvVar,
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValue,
    DatasetEnvVarsConfig,
    DeleteDatasetEnvVarError,
    GetDatasetEntryError,
    UpsertDatasetEnvVarStatus,
};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    GenericResourceQueryService,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeleteRequest,
    ResourceMetadataInput,
    UnsupportedResourceDescriptorError,
};
use kamu_resources_services::get_resource_crud_dispatcher_by_kind;
use secrecy::ExposeSecret;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
enum GetDispatcherError {
    #[error(transparent)]
    Unsupported(#[from] UnsupportedResourceDescriptorError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl From<GetDispatcherError> for InternalError {
    fn from(e: GetDispatcherError) -> Self {
        e.int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarMutationAdapter)]
pub struct DatasetEnvVarMutationAdapterImpl {
    catalog: dill::Catalog,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    variable_set_projection_repo: Arc<dyn VariableSetProjectionRepository>,
    secret_set_projection_repo: Arc<dyn SecretSetProjectionRepository>,
    variable_set_binding_repo: Arc<dyn DatasetVariableSetBindingRepository>,
    secret_set_binding_repo: Arc<dyn DatasetSecretSetBindingRepository>,
    dataset_env_var_config: Arc<DatasetEnvVarsConfig>,
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
        let dataset_entry = self
            .dataset_entry_repository
            .get_dataset_entry(dataset_id)
            .await
            .map_err(|e| match e {
                GetDatasetEntryError::NotFound(nf) => nf.int_err(),
                GetDatasetEntryError::Internal(e) => e,
            })?;

        let account_id = dataset_entry.owner_id;

        match value {
            DatasetEnvVarValue::Regular(plaintext) => {
                self.upsert_variable(dataset_id, key, plaintext, &account_id)
                    .await
            }
            DatasetEnvVarValue::Secret(secret) => {
                self.upsert_secret(dataset_id, key, secret.expose_secret(), &account_id)
                    .await
            }
        }
    }

    async fn delete_env_var_by_entry_id(
        &self,
        entry_id: &Uuid,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        // Try variable set first
        if let Some((resource_uid, key)) = self
            .variable_set_projection_repo
            .find_resource_uid_by_entry_id(entry_id)
            .await
            .int_err()?
        {
            return self.delete_variable(resource_uid, &key).await;
        }

        // Try secret set
        if let Some((resource_uid, key)) = self
            .secret_set_projection_repo
            .find_resource_uid_by_entry_id(entry_id)
            .await
            .int_err()?
        {
            return self.delete_secret(resource_uid, &key).await;
        }

        Err(DeleteDatasetEnvVarError::NotFound(
            DatasetEnvVarNotFoundError {
                dataset_env_var_key: entry_id.to_string(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetEnvVarMutationAdapterImpl {
    async fn upsert_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        plaintext: &str,
        account_id: &odf::AccountID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = format!("legacy-vars-{}", dataset_id.as_multibase());
        let dispatcher = get_resource_crud_dispatcher_by_kind::<GetDispatcherError>(
            &self.catalog,
            VariableSetResource::RESOURCE_TYPE,
        )
        .map_err(InternalError::from)?;

        let (existing_uid, mut variables, existing_entry_id) = self
            .load_existing_variable_spec(account_id, &resource_name, key)
            .await?;

        let exists_as_variable = variables.contains_key(key);

        // If key is new to variables, it may be a class conversion from secret →
        // variable
        let existed_as_secret = if !exists_as_variable {
            self.delete_if_secret(dataset_id, key).await?
        } else {
            false
        };

        let is_new_key = !exists_as_variable && !existed_as_secret;

        variables.insert(
            key.to_string(),
            VariableSpec::Literal(plaintext.to_string()),
        );

        let new_spec = serde_json::to_value(VariableSetSpec { variables }).int_err()?;
        let metadata = self.make_metadata(account_id.clone(), resource_name);

        let apply_decision = dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                uid: existing_uid,
                metadata,
                spec: new_spec,
            })
            .await
            .int_err()?;

        let resource_uid = match apply_decision {
            ApplyManifestApplicationDecision::Applied(result) => result.resource.metadata.uid,
            ApplyManifestApplicationDecision::Rejected(rejection) => {
                return Err(format!("VariableSet apply rejected: {}", rejection.message).int_err());
            }
        };

        self.variable_set_binding_repo
            .replace_bindings(dataset_id, &[resource_uid])
            .await
            .int_err()?;

        let entry_id = existing_entry_id.unwrap_or_else(Uuid::new_v4);

        let status = if is_new_key {
            UpsertDatasetEnvVarStatus::Created
        } else {
            UpsertDatasetEnvVarStatus::Updated
        };

        Ok(DatasetEnvVarUpsertResult {
            dataset_env_var: DatasetEnvVar {
                id: entry_id,
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
        account_id: &odf::AccountID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = format!("legacy-secrets-{}", dataset_id.as_multibase());
        let dispatcher = get_resource_crud_dispatcher_by_kind::<GetDispatcherError>(
            &self.catalog,
            SecretSetResource::RESOURCE_TYPE,
        )
        .map_err(InternalError::from)?;

        let (existing_uid, mut secrets, existing_entry_id) = self
            .load_existing_secret_spec_decrypted(account_id, &resource_name, key)
            .await?;

        let exists_as_secret = secrets.contains_key(key);

        // If key is new to secrets, it may be a class conversion from variable → secret
        let existed_as_variable = if !exists_as_secret {
            self.delete_if_variable(dataset_id, key).await?
        } else {
            false
        };
        let is_new_key = !exists_as_secret && !existed_as_variable;

        secrets.insert(key.to_string(), SecretSpec::Literal(plaintext.to_string()));

        let new_spec = serde_json::to_value(SecretSetSpec { secrets }).int_err()?;
        let metadata = self.make_metadata(account_id.clone(), resource_name);

        let apply_decision = dispatcher
            .apply(ResourceCrudDispatcherApplyRequest {
                uid: existing_uid,
                metadata,
                spec: new_spec,
            })
            .await
            .int_err()?;

        let resource_uid = match apply_decision {
            ApplyManifestApplicationDecision::Applied(result) => result.resource.metadata.uid,
            ApplyManifestApplicationDecision::Rejected(rejection) => {
                return Err(format!("SecretSet apply rejected: {}", rejection.message).int_err());
            }
        };

        self.secret_set_binding_repo
            .replace_bindings(dataset_id, &[resource_uid])
            .await
            .int_err()?;

        let entry_id = existing_entry_id.unwrap_or_else(Uuid::new_v4);

        let encryption_key = self
            .dataset_env_var_config
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
                id: entry_id,
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
        resource_uid: kamu_resources::ResourceUID,
        key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&resource_uid)
            .await
            .int_err()?
            .ok_or_else(|| {
                DeleteDatasetEnvVarError::Internal(
                    format!("VariableSet resource {resource_uid} not found in snapshot store")
                        .int_err(),
                )
            })?;

        let mut spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        spec.variables.remove(key);

        let dispatcher = get_resource_crud_dispatcher_by_kind::<GetDispatcherError>(
            &self.catalog,
            VariableSetResource::RESOURCE_TYPE,
        )
        .map_err(InternalError::from)?;

        if spec.variables.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.metadata.account.clone(),
                    uids: vec![resource_uid],
                })
                .await
                .int_err()?;
        } else {
            let metadata = ResourceMetadataInput {
                account: snapshot.metadata.account,
                name: snapshot.metadata.name,
                description: snapshot.metadata.description,
                labels: snapshot.metadata.labels,
                annotations: snapshot.metadata.annotations,
            };
            dispatcher
                .apply(ResourceCrudDispatcherApplyRequest {
                    uid: Some(resource_uid),
                    metadata,
                    spec: serde_json::to_value(spec).int_err()?,
                })
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn delete_secret(
        &self,
        resource_uid: kamu_resources::ResourceUID,
        key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&resource_uid)
            .await
            .int_err()?
            .ok_or_else(|| {
                DeleteDatasetEnvVarError::Internal(
                    format!("SecretSet resource {resource_uid} not found in snapshot store")
                        .int_err(),
                )
            })?;

        let mut decrypted = self.decrypt_secret_entries(&resource_uid).await.int_err()?;
        decrypted.remove(key);

        let dispatcher = get_resource_crud_dispatcher_by_kind::<GetDispatcherError>(
            &self.catalog,
            SecretSetResource::RESOURCE_TYPE,
        )
        .map_err(InternalError::from)?;

        if decrypted.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.metadata.account.clone(),
                    uids: vec![resource_uid],
                })
                .await
                .int_err()?;
        } else {
            let new_spec = SecretSetSpec {
                secrets: decrypted
                    .into_iter()
                    .map(|(k, v)| (k, SecretSpec::Literal(v)))
                    .collect(),
            };
            let metadata = ResourceMetadataInput {
                account: snapshot.metadata.account,
                name: snapshot.metadata.name,
                description: snapshot.metadata.description,
                labels: snapshot.metadata.labels,
                annotations: snapshot.metadata.annotations,
            };
            dispatcher
                .apply(ResourceCrudDispatcherApplyRequest {
                    uid: Some(resource_uid),
                    metadata,
                    spec: serde_json::to_value(new_spec).int_err()?,
                })
                .await
                .int_err()?;
        }

        Ok(())
    }

    /// Returns true if `key` existed as a secret for `dataset_id` and was
    /// deleted. Used when converting a key from secret to regular variable.
    async fn delete_if_secret(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
    ) -> Result<bool, InternalError> {
        let bindings = self
            .secret_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        for binding in bindings {
            let entries = self
                .secret_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await?;

            if entries.iter().any(|e| e.key == key) {
                self.delete_secret(binding.resource_uid, key)
                    .await
                    .map_err(|e| match e {
                        DeleteDatasetEnvVarError::NotFound(e) => e.int_err(),
                        DeleteDatasetEnvVarError::Internal(e) => e,
                    })?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Returns true if `key` existed as a variable for `dataset_id` and was
    /// deleted. Used when converting a key from regular variable to secret.
    async fn delete_if_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
    ) -> Result<bool, InternalError> {
        let bindings = self
            .variable_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        for binding in bindings {
            let entries = self
                .variable_set_projection_repo
                .get_latest_entries(&binding.resource_uid)
                .await?;

            if entries.iter().any(|e| e.key == key) {
                self.delete_variable(binding.resource_uid, key)
                    .await
                    .map_err(|e| match e {
                        DeleteDatasetEnvVarError::NotFound(e) => e.int_err(),
                        DeleteDatasetEnvVarError::Internal(e) => e,
                    })?;
                return Ok(true);
            }
        }

        Ok(false)
    }

    // Returns (existing_uid, variables_map, existing_entry_id_for_key)
    async fn load_existing_variable_spec(
        &self,
        account_id: &odf::AccountID,
        resource_name: &str,
        key: &str,
    ) -> Result<
        (
            Option<kamu_resources::ResourceUID>,
            BTreeMap<String, VariableSpec>,
            Option<Uuid>,
        ),
        InternalError,
    > {
        let uid = self
            .generic_resource_query_service
            .find_resource_uid_by_name(
                account_id,
                VariableSetResource::RESOURCE_TYPE,
                &resource_name.to_string(),
            )
            .await?;

        let Some(uid) = uid else {
            return Ok((None, BTreeMap::new(), None));
        };

        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&uid)
            .await?
            .ok_or_else(|| format!("VariableSet {uid} missing snapshot").int_err())?;

        let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

        // Look up the existing entry_id for this key from the projection
        let existing_entry_id = self
            .variable_set_projection_repo
            .get_latest_entries(&uid)
            .await?
            .into_iter()
            .find(|e| e.key == key)
            .map(|e| e.entry_id);

        Ok((Some(uid), spec.variables, existing_entry_id))
    }

    // Returns (existing_uid, decrypted_secrets_map, existing_entry_id_for_key)
    async fn load_existing_secret_spec_decrypted(
        &self,
        account_id: &odf::AccountID,
        resource_name: &str,
        key: &str,
    ) -> Result<
        (
            Option<kamu_resources::ResourceUID>,
            BTreeMap<String, SecretSpec>,
            Option<Uuid>,
        ),
        InternalError,
    > {
        let uid = self
            .generic_resource_query_service
            .find_resource_uid_by_name(
                account_id,
                SecretSetResource::RESOURCE_TYPE,
                &resource_name.to_string(),
            )
            .await?;

        let Some(uid) = uid else {
            return Ok((None, BTreeMap::new(), None));
        };

        let decrypted = self.decrypt_secret_entries(&uid).await?;

        let existing_entry_id = self
            .secret_set_projection_repo
            .get_latest_entries(&uid)
            .await?
            .into_iter()
            .find(|e| e.key == key)
            .map(|e| e.entry_id);

        let secrets = decrypted
            .into_iter()
            .map(|(k, v)| (k, SecretSpec::Literal(v)))
            .collect();

        Ok((Some(uid), secrets, existing_entry_id))
    }

    async fn decrypt_secret_entries(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
    ) -> Result<BTreeMap<String, String>, InternalError> {
        let encryption_key = self
            .dataset_env_var_config
            .encryption_key
            .as_deref()
            .unwrap_or("");
        let encryptor = AesGcmEncryptor::try_new(encryption_key).int_err()?;

        let entries = self
            .secret_set_projection_repo
            .get_latest_entries(resource_uid)
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

    fn make_metadata(
        &self,
        account_id: odf::AccountID,
        resource_name: String,
    ) -> ResourceMetadataInput {
        ResourceMetadataInput {
            account: account_id,
            name: resource_name,
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
