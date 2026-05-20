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
    ResourceMetadataInput,
    ResourceUID,
    UnsupportedResourceDescriptorError,
};
use kamu_resources_services::get_resource_crud_dispatcher_by_kind;
use secrecy::ExposeSecret;

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

    async fn delete_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_env_var_key: &str,
    ) -> Result<(), DeleteDatasetEnvVarError> {
        // Try deleting as a variable
        let was_variable = self
            .delete_if_variable(dataset_id, dataset_env_var_key)
            .await?;
        if was_variable {
            return Ok(());
        }

        // Maybe it's a secret then?
        let was_secret = self
            .delete_if_secret(dataset_id, dataset_env_var_key)
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
    pub fn legacy_variable_set_resource_name(dataset_id: &odf::DatasetID) -> String {
        format!("legacy-vars-{}", dataset_id.as_multibase())
    }

    pub fn legacy_secret_set_resource_name(dataset_id: &odf::DatasetID) -> String {
        format!("legacy-secrets-{}", dataset_id.as_multibase())
    }

    fn get_dispatcher<E>(&self, resource_type: &str) -> Result<Arc<dyn ResourceCrudDispatcher>, E>
    where
        E: From<InternalError>,
    {
        get_resource_crud_dispatcher_by_kind::<GetDispatcherError>(&self.catalog, resource_type)
            .map_err(InternalError::from)
            .map_err(E::from)
    }

    async fn apply_and_handle_rejection<E>(
        &self,
        dispatcher: &Arc<dyn ResourceCrudDispatcher>,
        request: ResourceCrudDispatcherApplyRequest,
        resource_type_name: &str,
    ) -> Result<ResourceUID, E>
    where
        E: From<InternalError>,
    {
        let apply_decision = dispatcher.apply(request).await.int_err().map_err(E::from)?;

        match apply_decision {
            ApplyManifestApplicationDecision::Applied(result) => Ok(result.resource.metadata.uid),
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
        account_id: &odf::AccountID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = Self::legacy_variable_set_resource_name(dataset_id);
        let (existing_uid, mut variables) = self
            .load_existing_variable_spec(account_id, &resource_name)
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

        let dispatcher =
            self.get_dispatcher::<InternalError>(VariableSetResource::RESOURCE_TYPE)?;

        let resource_uid = self
            .apply_and_handle_rejection::<InternalError>(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    uid: existing_uid,
                    metadata,
                    spec: new_spec,
                },
                "VariableSet",
            )
            .await?;

        self.variable_set_binding_repo
            .replace_bindings(dataset_id, &[resource_uid])
            .await
            .int_err()?;

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
        account_id: &odf::AccountID,
    ) -> Result<DatasetEnvVarUpsertResult, InternalError> {
        let resource_name = Self::legacy_secret_set_resource_name(dataset_id);

        let (existing_uid, mut secrets) = self
            .load_existing_secret_spec_decrypted(account_id, &resource_name)
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

        let dispatcher = self.get_dispatcher::<InternalError>(SecretSetResource::RESOURCE_TYPE)?;

        let resource_uid = self
            .apply_and_handle_rejection::<InternalError>(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    uid: existing_uid,
                    metadata,
                    spec: new_spec,
                },
                "SecretSet",
            )
            .await?;

        self.secret_set_binding_repo
            .replace_bindings(dataset_id, &[resource_uid])
            .await
            .int_err()?;

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
        dataset_id: &odf::DatasetID,
        resource_uid: ResourceUID,
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

        let dispatcher = self
            .get_dispatcher(VariableSetResource::RESOURCE_TYPE)
            .map_err(DeleteDatasetEnvVarError::Internal)?;

        if spec.variables.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.metadata.account.clone(),
                    uids: vec![resource_uid],
                })
                .await
                .int_err()?;
            // Remove the binding now that the managed resource is gone
            self.variable_set_binding_repo
                .replace_bindings(dataset_id, &[])
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
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    uid: Some(resource_uid),
                    metadata,
                    spec: serde_json::to_value(spec).int_err()?,
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
        dataset_id: &odf::DatasetID,
        resource_uid: ResourceUID,
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

        let dispatcher = self
            .get_dispatcher(SecretSetResource::RESOURCE_TYPE)
            .map_err(DeleteDatasetEnvVarError::Internal)?;

        if decrypted.is_empty() {
            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: snapshot.metadata.account.clone(),
                    uids: vec![resource_uid],
                })
                .await
                .int_err()?;
            // Remove the binding now that the managed resource is gone
            self.secret_set_binding_repo
                .replace_bindings(dataset_id, &[])
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
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    uid: Some(resource_uid),
                    metadata,
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
        key: &str,
    ) -> Result<bool, InternalError> {
        match self.find_secret(dataset_id, key).await? {
            Some((resource_uid, key)) => self
                .delete_secret(dataset_id, resource_uid, &key)
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
        key: &str,
    ) -> Result<bool, InternalError> {
        match self.find_variable(dataset_id, key).await? {
            Some((resource_uid, key)) => self
                .delete_variable(dataset_id, resource_uid, &key)
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
        key: &str,
    ) -> Result<Option<(ResourceUID, String)>, InternalError> {
        let bindings = self
            .secret_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        match bindings.len() {
            0 => return Ok(None),
            1 => {} // expected case, continue with lookup
            _ => {
                return Err(format!(
                    "Multiple SecretSet bindings found for dataset {dataset_id}: {bindings:?}. \
                     This does not qualify for legacy env var resolution",
                )
                .int_err());
            }
        }

        let the_binding = &bindings[0];

        let entries = self
            .secret_set_projection_repo
            .get_latest_entries(&the_binding.resource_uid)
            .await?;

        if entries.iter().any(|e| e.key == key) {
            return Ok(Some((the_binding.resource_uid, key.to_string())));
        }

        Ok(None)
    }

    async fn find_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
    ) -> Result<Option<(ResourceUID, String)>, InternalError> {
        let bindings = self
            .variable_set_binding_repo
            .list_bindings(dataset_id)
            .await?;

        match bindings.len() {
            0 => return Ok(None),
            1 => {} // expected case, continue with lookup
            _ => {
                return Err(format!(
                    "Multiple VariableSet bindings found for dataset {dataset_id}: {bindings:?}. \
                     This does not qualify for legacy env var resolution",
                )
                .int_err());
            }
        }

        let the_binding = &bindings[0];

        let entries = self
            .variable_set_projection_repo
            .get_latest_entries(&the_binding.resource_uid)
            .await?;

        if entries.iter().any(|e| e.key == key) {
            return Ok(Some((the_binding.resource_uid, key.to_string())));
        }

        Ok(None)
    }

    // Returns (existing_uid, variables_map, existing_entry_id_for_key)
    async fn load_existing_variable_spec(
        &self,
        account_id: &odf::AccountID,
        resource_name: &str,
    ) -> Result<(Option<ResourceUID>, BTreeMap<String, VariableSpec>), InternalError> {
        let uid = self
            .generic_resource_query_service
            .find_resource_uid_by_name(
                account_id,
                VariableSetResource::RESOURCE_TYPE,
                &resource_name.to_string(),
            )
            .await?;

        let Some(uid) = uid else {
            return Ok((None, BTreeMap::new()));
        };

        let snapshot = self
            .generic_resource_query_service
            .get_snapshot_by_uid(&uid)
            .await?
            .ok_or_else(|| format!("VariableSet {uid} missing snapshot").int_err())?;

        let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        Ok((Some(uid), spec.variables))
    }

    // Returns (existing_uid, decrypted_secrets_map)
    async fn load_existing_secret_spec_decrypted(
        &self,
        account_id: &odf::AccountID,
        resource_name: &str,
    ) -> Result<(Option<ResourceUID>, BTreeMap<String, SecretSpec>), InternalError> {
        let uid = self
            .generic_resource_query_service
            .find_resource_uid_by_name(
                account_id,
                SecretSetResource::RESOURCE_TYPE,
                &resource_name.to_string(),
            )
            .await?;

        let Some(uid) = uid else {
            return Ok((None, BTreeMap::new()));
        };

        let decrypted = self.decrypt_secret_entries(&uid).await?;

        let secrets = decrypted
            .into_iter()
            .map(|(k, v)| (k, SecretSpec::Literal(v)))
            .collect();

        Ok((Some(uid), secrets))
    }

    async fn decrypt_secret_entries(
        &self,
        resource_uid: &ResourceUID,
    ) -> Result<BTreeMap<String, String>, InternalError> {
        let encryption_key = self
            .secrets_encryption_config
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
