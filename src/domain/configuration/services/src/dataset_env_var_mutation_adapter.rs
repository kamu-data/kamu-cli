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
    SecretExt,
    SecretSetResource,
    SecretSetSpec,
    SecretSetSpecInput,
    Variable,
    VariableSetResource,
    VariableSetSpec,
    VariableSetSpecInput,
};
use kamu_datasets::{
    CreateDatasetEnvVarError,
    DatasetEntry,
    DatasetEntryRepository,
    DatasetEnvVar,
    DatasetEnvVarAlreadyExistsError,
    DatasetEnvVarConcurrentModificationError,
    DatasetEnvVarMutationAdapter,
    DatasetEnvVarNotFoundError,
    DatasetEnvVarSpecResolver,
    DatasetEnvVarUpsertResult,
    DatasetEnvVarValidationError,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    GetDatasetEntryError,
    GetDatasetEnvVarError,
    SecretsEncryptionConfig,
    UpsertDatasetEnvVarError,
    UpsertDatasetEnvVarStatus,
};
use kamu_resources::{
    ApplyManifestApplicationDecision,
    ApplyResourceCrudDispatcherError,
    ApplyResourcePrecondition,
    GenericResourceQueryService,
    ResourceCrudDispatcher,
    ResourceCrudDispatcherApplyRequest,
    ResourceCrudDispatcherDeletionRequest,
    ResourceHeadersExt,
    ResourceHeadersInput,
    ResourceID,
    ResourceName,
    ResourceSchemaProvider,
    ResourceSnapshot,
    ResourceSpecFromInput,
    TypeUri,
};
use kamu_resources_services::ResourceDispatcherFactory;
use secrecy::ExposeSecret;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarMutationAdapterImpl {
    dispatcher_factory: Arc<ResourceDispatcherFactory>,
    dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
    account_service: Arc<dyn AccountService>,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
    spec_resolver: Arc<dyn DatasetEnvVarSpecResolver>,
    secrets_encryption_config: Arc<SecretsEncryptionConfig>,
}

#[dill::component(pub)]
#[dill::interface(dyn DatasetEnvVarMutationAdapter)]
impl DatasetEnvVarMutationAdapterImpl {
    pub fn new(
        dispatcher_factory: Arc<ResourceDispatcherFactory>,
        dataset_entry_repository: Arc<dyn DatasetEntryRepository>,
        account_service: Arc<dyn AccountService>,
        generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
        spec_resolver: Arc<dyn DatasetEnvVarSpecResolver>,
        secrets_encryption_config: Arc<SecretsEncryptionConfig>,
    ) -> Self {
        Self {
            dispatcher_factory,
            dataset_entry_repository,
            account_service,
            generic_resource_query_service,
            spec_resolver,
            secrets_encryption_config,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEnvVarMutationAdapter for DatasetEnvVarMutationAdapterImpl {
    async fn upsert_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &DatasetEnvVarValue,
    ) -> Result<DatasetEnvVarUpsertResult, UpsertDatasetEnvVarError> {
        let dataset_entry = self.get_dataset_entry(dataset_id).await?;

        self.write_env_var(&dataset_entry, key, value, EnvVarWriteMode::Upsert)
            .await
            .map_err(|err| match err {
                // Only a create refuses an existing key
                CreateDatasetEnvVarError::AlreadyExists(e) => {
                    UpsertDatasetEnvVarError::Internal(e.int_err())
                }
                CreateDatasetEnvVarError::Validation(e) => UpsertDatasetEnvVarError::Validation(e),
                CreateDatasetEnvVarError::ConcurrentModification(e) => {
                    UpsertDatasetEnvVarError::ConcurrentModification(e)
                }
                CreateDatasetEnvVarError::Internal(e) => UpsertDatasetEnvVarError::Internal(e),
            })
    }

    async fn create_env_var(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        value: &DatasetEnvVarValue,
    ) -> Result<DatasetEnvVar, CreateDatasetEnvVarError> {
        let dataset_entry = self.get_dataset_entry(dataset_id).await?;

        // A key is taken when the dataset's environment resolves it, from the
        // legacy sets or from any other set targeting the dataset. The write
        // re-checks the legacy sets under its precondition, so a key created in
        // them after this lookup is still refused.
        match self
            .spec_resolver
            .get_env_var_by_entry_key(dataset_id, key)
            .await
        {
            Ok(_) => return Err(Self::already_exists(key)),
            Err(GetDatasetEnvVarError::NotFound(_)) => {}
            Err(GetDatasetEnvVarError::Internal(err)) => return Err(err.into()),
        }

        let upsert_result = self
            .write_env_var(&dataset_entry, key, value, EnvVarWriteMode::Create)
            .await?;
        Ok(upsert_result.dataset_env_var)
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

    /// Writes a set read earlier, as long as it is still in the state
    /// `precondition` describes, so a concurrent write is never overwritten
    async fn apply_and_handle_rejection(
        &self,
        dispatcher: &Arc<dyn ResourceCrudDispatcher>,
        request: ResourceCrudDispatcherApplyRequest,
        precondition: ApplyResourcePrecondition,
        resource_type_name: &str,
    ) -> Result<ResourceID, EnvVarSetWriteError> {
        let apply_decision = match dispatcher
            .apply_with_precondition(request, precondition)
            .await
        {
            Ok(apply_decision) => apply_decision,
            Err(ApplyResourceCrudDispatcherError::ConcurrentModification(_)) => {
                return Err(EnvVarSetWriteError::ConcurrentModification);
            }
            Err(
                err @ (ApplyResourceCrudDispatcherError::InvalidSpec { .. }
                | ApplyResourceCrudDispatcherError::NotFound(_)
                | ApplyResourceCrudDispatcherError::TypeMismatch(_)
                | ApplyResourceCrudDispatcherError::Internal(_)),
            ) => return Err(err.int_err().into()),
        };

        match apply_decision {
            ApplyManifestApplicationDecision::Applied(result) => Ok(result.resource.headers.id),
            ApplyManifestApplicationDecision::Rejected(rejection) => Err(format!(
                "{resource_type_name} apply rejected: {}",
                rejection.message
            )
            .int_err()
            .into()),
        }
    }

    async fn write_env_var(
        &self,
        dataset_entry: &DatasetEntry,
        key: &str,
        value: &DatasetEnvVarValue,
        mode: EnvVarWriteMode,
    ) -> Result<DatasetEnvVarUpsertResult, CreateDatasetEnvVarError> {
        match value {
            DatasetEnvVarValue::Regular(plaintext) => {
                self.upsert_variable(
                    &dataset_entry.id,
                    key,
                    plaintext,
                    &dataset_entry.owner_id,
                    &dataset_entry.owner_name,
                    mode,
                )
                .await
            }
            DatasetEnvVarValue::Secret(secret) => {
                self.upsert_secret(
                    &dataset_entry.id,
                    key,
                    secret.expose_secret(),
                    &dataset_entry.owner_id,
                    &dataset_entry.owner_name,
                    mode,
                )
                .await
            }
        }
    }

    async fn upsert_variable(
        &self,
        dataset_id: &odf::DatasetID,
        key: &str,
        plaintext: &str,
        account_did: &odf::AccountID,
        account_name: &odf::AccountName,
        mode: EnvVarWriteMode,
    ) -> Result<DatasetEnvVarUpsertResult, CreateDatasetEnvVarError> {
        let resource_name = Self::legacy_variable_set_resource_name(dataset_id);
        let LoadedEnvVarSet {
            id: existing_id,
            precondition,
            entries: mut variables,
        } = self.load_variable_set(account_did, &resource_name).await?;

        let exists_as_variable = variables.contains_key(key);

        let existed_as_secret = match mode {
            EnvVarWriteMode::Upsert => {
                EnvVarSetRules::VARIABLES.check_entry(&variables, key, plaintext)?;

                // If key is new to variables, it may be a class conversion from
                // secret → variable
                if exists_as_variable {
                    false
                } else {
                    self.delete_if_secret(dataset_id, account_did, key).await?
                }
            }
            EnvVarWriteMode::Create => {
                if exists_as_variable
                    || self
                        .find_secret(dataset_id, account_did, key)
                        .await?
                        .is_some()
                {
                    return Err(Self::already_exists(key));
                }
                EnvVarSetRules::VARIABLES.check_entry(&variables, key, plaintext)?;
                false
            }
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

        self.apply_and_handle_rejection(
            &dispatcher,
            ResourceCrudDispatcherApplyRequest {
                id: existing_id,
                headers,
                spec: new_spec,
            },
            precondition,
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
        mode: EnvVarWriteMode,
    ) -> Result<DatasetEnvVarUpsertResult, CreateDatasetEnvVarError> {
        let resource_name = Self::legacy_secret_set_resource_name(dataset_id);

        let LoadedEnvVarSet {
            id: existing_id,
            precondition,
            entries: mut secrets,
        } = self.load_secret_set(account_did, &resource_name).await?;

        let exists_as_secret = secrets.contains_key(key);

        let existed_as_variable = match mode {
            EnvVarWriteMode::Upsert => {
                EnvVarSetRules::SECRETS.check_entry(&secrets, key, plaintext)?;

                // If key is new to secrets, it may be a class conversion from
                // variable → secret
                if exists_as_secret {
                    false
                } else {
                    self.delete_if_variable(dataset_id, account_did, key)
                        .await?
                }
            }
            EnvVarWriteMode::Create => {
                if exists_as_secret
                    || self
                        .find_variable(dataset_id, account_did, key)
                        .await?
                        .is_some()
                {
                    return Err(Self::already_exists(key));
                }
                EnvVarSetRules::SECRETS.check_entry(&secrets, key, plaintext)?;
                false
            }
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

        self.apply_and_handle_rejection(
            &dispatcher,
            ResourceCrudDispatcherApplyRequest {
                id: existing_id,
                headers,
                spec: new_spec,
            },
            precondition,
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

    fn already_exists(key: &str) -> CreateDatasetEnvVarError {
        CreateDatasetEnvVarError::AlreadyExists(DatasetEnvVarAlreadyExistsError {
            dataset_env_var_key: key.to_string(),
        })
    }

    async fn delete_variable(
        &self,
        resource_id: ResourceID,
        key: &str,
    ) -> Result<(), EnvVarSetWriteError> {
        let snapshot = self.load_snapshot(&resource_id).await?;
        let precondition = ApplyResourcePrecondition::Generation(snapshot.headers.generation);

        let mut spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        spec.variables.entries.remove(key);

        let dispatcher = self.get_dispatcher(VariableSetResource::SCHEMA_STR)?;

        if spec.variables.entries.is_empty() {
            dispatcher
                .request_deletion(ResourceCrudDispatcherDeletionRequest {
                    account_id: snapshot.headers.account.did.clone(),
                    ids: vec![resource_id],
                })
                .await
                .int_err()?;
        } else {
            let headers = snapshot.headers.into_input();
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    id: Some(resource_id),
                    headers,
                    spec: serde_json::to_value(spec.into_input()).int_err()?,
                },
                precondition,
                "VariableSet",
            )
            .await?;
        }

        Ok(())
    }

    async fn delete_secret(
        &self,
        resource_id: ResourceID,
        key: &str,
    ) -> Result<(), EnvVarSetWriteError> {
        let snapshot = self.load_snapshot(&resource_id).await?;
        let precondition = ApplyResourcePrecondition::Generation(snapshot.headers.generation);

        let spec: SecretSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        let mut decrypted = self.decrypt_secret_entries(spec.into_dto())?;
        decrypted.remove(key);

        let dispatcher = self.get_dispatcher(SecretSetResource::SCHEMA_STR)?;

        if decrypted.is_empty() {
            dispatcher
                .request_deletion(ResourceCrudDispatcherDeletionRequest {
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
            let headers = snapshot.headers.into_input();
            self.apply_and_handle_rejection(
                &dispatcher,
                ResourceCrudDispatcherApplyRequest {
                    id: Some(resource_id),
                    headers,
                    spec: serde_json::to_value(new_spec).int_err()?,
                },
                precondition,
                "SecretSet",
            )
            .await?;
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
    ) -> Result<bool, EnvVarSetWriteError> {
        match self.find_secret(dataset_id, owner_id, key).await? {
            Some((resource_id, key)) => self.delete_secret(resource_id, &key).await.map(|_| true),
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
    ) -> Result<bool, EnvVarSetWriteError> {
        match self.find_variable(dataset_id, owner_id, key).await? {
            Some((resource_id, key)) => self.delete_variable(resource_id, &key).await.map(|_| true),
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

        let spec = self.load_secret_spec(&resource_id).await?;

        if spec.secrets.entries.contains_key(key) {
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

        let spec = self.load_variable_spec(&resource_id).await?;

        if spec.variables.entries.contains_key(key) {
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

    async fn load_variable_set(
        &self,
        account_id: &odf::AccountID,
        resource_name: &ResourceName,
    ) -> Result<LoadedEnvVarSet<Variable>, InternalError> {
        let Some(id) = self
            .find_legacy_resource_id(account_id, VariableSetResource::schema(), resource_name)
            .await?
        else {
            return Ok(LoadedEnvVarSet::absent());
        };

        let snapshot = self.load_snapshot(&id).await?;
        let generation = snapshot.headers.generation;
        let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

        Ok(LoadedEnvVarSet {
            id: Some(id),
            precondition: ApplyResourcePrecondition::Generation(generation),
            entries: spec.into_dto().variables.entries,
        })
    }

    /// Like [`Self::load_variable_set`], with the secrets decrypted
    async fn load_secret_set(
        &self,
        account_id: &odf::AccountID,
        resource_name: &ResourceName,
    ) -> Result<LoadedEnvVarSet<Secret>, InternalError> {
        let Some(id) = self
            .find_legacy_resource_id(account_id, SecretSetResource::schema(), resource_name)
            .await?
        else {
            return Ok(LoadedEnvVarSet::absent());
        };

        let snapshot = self.load_snapshot(&id).await?;
        let generation = snapshot.headers.generation;
        let spec: SecretSetSpec = serde_json::from_value(snapshot.spec).int_err()?;

        let entries = self
            .decrypt_secret_entries(spec.into_dto())?
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

        Ok(LoadedEnvVarSet {
            id: Some(id),
            precondition: ApplyResourcePrecondition::Generation(generation),
            entries,
        })
    }

    /// The set as stored in its spec.
    ///
    /// Not the projection: this feeds read-modify-write applies, so a
    /// pre-reconcile read would rebuild the spec without any entry added since
    /// and silently retract it.
    async fn load_snapshot(
        &self,
        resource_id: &ResourceID,
    ) -> Result<ResourceSnapshot, InternalError> {
        self.generic_resource_query_service
            .get_snapshot_by_id(resource_id)
            .await?
            .ok_or_else(|| format!("Resource {resource_id} missing snapshot").int_err())
    }

    async fn load_variable_spec(
        &self,
        resource_id: &ResourceID,
    ) -> Result<odf::metadata::config::VariableSetSpec, InternalError> {
        let snapshot = self.load_snapshot(resource_id).await?;
        let spec: VariableSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        Ok(spec.into_dto())
    }

    async fn load_secret_spec(
        &self,
        resource_id: &ResourceID,
    ) -> Result<odf::metadata::config::SecretSetSpec, InternalError> {
        let snapshot = self.load_snapshot(resource_id).await?;
        let spec: SecretSetSpec = serde_json::from_value(snapshot.spec).int_err()?;
        Ok(spec.into_dto())
    }

    /// The set's secrets as plaintext
    fn decrypt_secret_entries(
        &self,
        spec: odf::metadata::config::SecretSetSpec,
    ) -> Result<BTreeMap<String, String>, InternalError> {
        let cryptor = self.secrets_encryption_config.new_secret_cryptor()?;

        let mut decrypted = BTreeMap::new();
        for (key, secret) in spec.secrets.entries {
            let plaintext = secret.decrypt_plaintext_bytes(&cryptor)?;
            let plaintext = String::from_utf8(plaintext).int_err()?;
            decrypted.insert(key, plaintext);
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
            account: Some(odf::metadata::auth::ResourceOwnerRef {
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
            owner_references: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A legacy set read for a read-modify-write, with the precondition its write
/// must carry
struct LoadedEnvVarSet<T> {
    id: Option<ResourceID>,
    precondition: ApplyResourcePrecondition,
    entries: BTreeMap<String, T>,
}

impl<T> LoadedEnvVarSet<T> {
    fn absent() -> Self {
        Self {
            id: None,
            precondition: ApplyResourcePrecondition::Absent,
            entries: BTreeMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy)]
enum EnvVarWriteMode {
    /// Adds the key, or replaces it, converting it between variable and
    /// secret when its class changes
    Upsert,
    /// Adds the key, refusing one that either legacy set already holds
    Create,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

enum EnvVarSetWriteError {
    /// The set changed since it was read; nothing was written
    ConcurrentModification,
    Internal(InternalError),
}

impl From<InternalError> for EnvVarSetWriteError {
    fn from(err: InternalError) -> Self {
        Self::Internal(err)
    }
}

impl From<EnvVarSetWriteError> for UpsertDatasetEnvVarError {
    fn from(err: EnvVarSetWriteError) -> Self {
        match err {
            EnvVarSetWriteError::ConcurrentModification => {
                Self::ConcurrentModification(DatasetEnvVarConcurrentModificationError)
            }
            EnvVarSetWriteError::Internal(err) => Self::Internal(err),
        }
    }
}

impl From<EnvVarSetWriteError> for CreateDatasetEnvVarError {
    fn from(err: EnvVarSetWriteError) -> Self {
        UpsertDatasetEnvVarError::from(err).into()
    }
}

impl From<EnvVarSetWriteError> for DeleteDatasetEnvVarError {
    fn from(err: EnvVarSetWriteError) -> Self {
        match err {
            EnvVarSetWriteError::ConcurrentModification => {
                Self::ConcurrentModification(DatasetEnvVarConcurrentModificationError)
            }
            EnvVarSetWriteError::Internal(err) => Self::Internal(err),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The rules `validate()` of a variable or secret set applies to an entry
/// being written. Checked up front so a rejected key or value reaches the
/// caller as a typed error rather than as an apply rejection, and only for
/// the written entry, so legacy entries of the set do not block the write.
struct EnvVarSetRules {
    is_valid_key: fn(&str) -> bool,
    max_entries: usize,
    max_value_len: usize,
}

impl EnvVarSetRules {
    const VARIABLES: Self = Self {
        is_valid_key: VariableSetSpecInput::is_valid_variable_name,
        max_entries: VariableSetSpecInput::MAX_VARIABLES,
        max_value_len: VariableSetSpecInput::MAX_VARIABLE_VALUE_LEN,
    };

    const SECRETS: Self = Self {
        is_valid_key: SecretSetSpecInput::is_valid_secret_name,
        max_entries: SecretSetSpecInput::MAX_SECRETS,
        max_value_len: SecretSetSpecInput::MAX_SECRET_VALUE_LEN,
    };

    fn check_entry<T>(
        &self,
        entries: &BTreeMap<String, T>,
        key: &str,
        value: &str,
    ) -> Result<(), DatasetEnvVarValidationError> {
        let is_new_key = !entries.contains_key(key);

        if is_new_key && !(self.is_valid_key)(key) {
            return Err(DatasetEnvVarValidationError::InvalidKey {
                key: key.to_string(),
            });
        }

        if value.is_empty() {
            return Err(DatasetEnvVarValidationError::EmptyValue {
                key: key.to_string(),
            });
        }

        if value.len() > self.max_value_len {
            return Err(DatasetEnvVarValidationError::ValueTooLong {
                key: key.to_string(),
                actual: value.len(),
                max: self.max_value_len,
            });
        }

        if is_new_key && entries.len() >= self.max_entries {
            return Err(DatasetEnvVarValidationError::TooManyEntries {
                max: self.max_entries,
            });
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
