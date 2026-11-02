// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{
    CreateDatasetEnvVarError,
    DatasetEnvVarService,
    DatasetEnvVarValidationError,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    UpsertDatasetEnvVarError,
    UpsertDatasetEnvVarStatus,
};
use secrecy::SecretString;

use crate::prelude::*;
use crate::queries::{DatasetRequestState, ViewDatasetEnvVar};
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarsMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

/// Mutations that manage environment variables of a dataset
#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetEnvVarsMut<'a> {
    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::ensure_secrets_encryption_enabled(ctx)?;
        utils::check_dataset_maintain_access(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    /// Creates an environment variable, unless its key is already taken
    #[tracing::instrument(level = "info", name = DatasetEnvVarsMut_create_env_variable, skip_all)]
    async fn create_env_variable(
        &self,
        ctx: &Context<'_>,
        key: String,
        value: String,
        is_secret: bool,
    ) -> Result<CreateDatasetEnvVarResult> {
        let dataset_id = self.dataset_request_state.dataset_id();

        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);
        match dataset_env_var_service
            .create_dataset_env_var(dataset_id, &key, &Self::env_var_value(value, is_secret))
            .await
        {
            Ok(dataset_env_var) => Ok(CreateDatasetEnvVarResult::Created(
                CreateDatasetEnvVarResultCreated {
                    env_var: ViewDatasetEnvVar::new(dataset_env_var),
                },
            )),
            Err(CreateDatasetEnvVarError::AlreadyExists(_)) => {
                Ok(CreateDatasetEnvVarResult::AlreadyExists(
                    CreateDatasetEnvVarResultAlreadyExists { env_var_key: key },
                ))
            }
            Err(CreateDatasetEnvVarError::Validation(err)) => Ok(err.into()),
            Err(CreateDatasetEnvVarError::Internal(err)) => Err(GqlError::Internal(err)),
        }
    }

    /// Creates or updates an environment variable, optionally as a secret
    #[tracing::instrument(level = "info", name = DatasetEnvVarsMut_upsert_env_variable, skip_all)]
    async fn upsert_env_variable(
        &self,
        ctx: &Context<'_>,
        key: String,
        value: String,
        is_secret: bool,
    ) -> Result<UpsertDatasetEnvVarResult> {
        let dataset_id = self.dataset_request_state.dataset_id();

        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);
        let upsert_result = match dataset_env_var_service
            .upsert_dataset_env_var(dataset_id, &key, &Self::env_var_value(value, is_secret))
            .await
        {
            Ok(upsert_result) => upsert_result,
            Err(UpsertDatasetEnvVarError::Validation(err)) => return Ok(err.into()),
            Err(UpsertDatasetEnvVarError::Internal(err)) => return Err(GqlError::Internal(err)),
        };

        Ok(match upsert_result.status {
            UpsertDatasetEnvVarStatus::Created => {
                UpsertDatasetEnvVarResult::Created(UpsertDatasetEnvVarResultCreated {
                    env_var: ViewDatasetEnvVar::new(upsert_result.dataset_env_var),
                })
            }
            UpsertDatasetEnvVarStatus::Updated => {
                UpsertDatasetEnvVarResult::Updated(UpsertDatasetEnvVarResultUpdated {
                    env_var: ViewDatasetEnvVar::new(upsert_result.dataset_env_var),
                })
            }
            UpsertDatasetEnvVarStatus::UpToDate => {
                UpsertDatasetEnvVarResult::UpToDate(UpsertDatasetEnvVarUpToDate)
            }
        })
    }

    /// Deletes an environment variable by its key
    #[tracing::instrument(level = "info", name = DatasetEnvVarsMut_delete_env_variable, skip_all)]
    async fn delete_env_variable(
        &self,
        ctx: &Context<'_>,
        key: String,
    ) -> Result<DeleteDatasetEnvVarResult> {
        let dataset_id = self.dataset_request_state.dataset_id();

        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);

        match dataset_env_var_service
            .delete_dataset_env_var(dataset_id, &key)
            .await
        {
            Ok(_) => Ok(DeleteDatasetEnvVarResult::Success(
                DeleteDatasetEnvVarResultSuccess { env_var_key: key },
            )),
            Err(err) => match err {
                DeleteDatasetEnvVarError::NotFound(_) => Ok(DeleteDatasetEnvVarResult::NotFound(
                    DeleteDatasetEnvVarResultNotFound { env_var_key: key },
                )),
                DeleteDatasetEnvVarError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }

    #[graphql(skip)]
    fn env_var_value(value: String, is_secret: bool) -> DatasetEnvVarValue {
        if is_secret {
            DatasetEnvVarValue::Secret(SecretString::from(value))
        } else {
            DatasetEnvVarValue::Regular(value)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of creating an environment variable
#[derive(Interface, Debug)]
#[graphql(field(
    name = "message",
    ty = "String",
    desc = "Human-readable outcome message"
))]
pub enum CreateDatasetEnvVarResult {
    Created(CreateDatasetEnvVarResultCreated),
    AlreadyExists(CreateDatasetEnvVarResultAlreadyExists),
    InvalidKey(DatasetEnvVarInvalidKey),
    EmptyValue(DatasetEnvVarEmptyValue),
    ValueTooLong(DatasetEnvVarValueTooLong),
    TooManyEntries(DatasetEnvVarTooManyEntries),
}

impl From<DatasetEnvVarValidationError> for CreateDatasetEnvVarResult {
    fn from(err: DatasetEnvVarValidationError) -> Self {
        let message = err.to_string();
        match err {
            DatasetEnvVarValidationError::InvalidKey { key } => {
                Self::InvalidKey(DatasetEnvVarInvalidKey::new(key, message))
            }
            DatasetEnvVarValidationError::EmptyValue { key } => {
                Self::EmptyValue(DatasetEnvVarEmptyValue::new(key, message))
            }
            DatasetEnvVarValidationError::ValueTooLong { key, actual, max } => {
                Self::ValueTooLong(DatasetEnvVarValueTooLong::new(key, actual, max, message))
            }
            DatasetEnvVarValidationError::TooManyEntries { max } => {
                Self::TooManyEntries(DatasetEnvVarTooManyEntries::new(max, message))
            }
        }
    }
}

/// Environment variable was created
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetEnvVarResultCreated {
    /// Created environment variable
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl CreateDatasetEnvVarResultCreated {
    /// Human-readable outcome message
    async fn message(&self) -> String {
        "Created".to_string()
    }
}

/// Environment variable or secret with the given key already exists
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetEnvVarResultAlreadyExists {
    /// Key that already exists
    pub env_var_key: String,
}

#[ComplexObject]
impl CreateDatasetEnvVarResultAlreadyExists {
    /// Human-readable error message
    async fn message(&self) -> String {
        format!(
            "Environment variable with '{}' key already exists",
            self.env_var_key
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of creating or updating an environment variable
#[derive(Interface, Debug)]
#[graphql(field(
    name = "message",
    ty = "String",
    desc = "Human-readable outcome message"
))]
pub enum UpsertDatasetEnvVarResult {
    Created(UpsertDatasetEnvVarResultCreated),
    Updated(UpsertDatasetEnvVarResultUpdated),
    UpToDate(UpsertDatasetEnvVarUpToDate),
    InvalidKey(DatasetEnvVarInvalidKey),
    EmptyValue(DatasetEnvVarEmptyValue),
    ValueTooLong(DatasetEnvVarValueTooLong),
    TooManyEntries(DatasetEnvVarTooManyEntries),
}

impl From<DatasetEnvVarValidationError> for UpsertDatasetEnvVarResult {
    fn from(err: DatasetEnvVarValidationError) -> Self {
        let message = err.to_string();
        match err {
            DatasetEnvVarValidationError::InvalidKey { key } => {
                Self::InvalidKey(DatasetEnvVarInvalidKey::new(key, message))
            }
            DatasetEnvVarValidationError::EmptyValue { key } => {
                Self::EmptyValue(DatasetEnvVarEmptyValue::new(key, message))
            }
            DatasetEnvVarValidationError::ValueTooLong { key, actual, max } => {
                Self::ValueTooLong(DatasetEnvVarValueTooLong::new(key, actual, max, message))
            }
            DatasetEnvVarValidationError::TooManyEntries { max } => {
                Self::TooManyEntries(DatasetEnvVarTooManyEntries::new(max, message))
            }
        }
    }
}

#[derive(Debug)]
pub struct UpsertDatasetEnvVarUpToDate;

/// Environment variable already had the requested value
#[Object]
impl UpsertDatasetEnvVarUpToDate {
    /// Human-readable outcome message
    pub async fn message(&self) -> String {
        "Dataset env var is up to date".to_string()
    }
}

/// Environment variable was created
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpsertDatasetEnvVarResultCreated {
    /// Created environment variable
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl UpsertDatasetEnvVarResultCreated {
    /// Human-readable outcome message
    async fn message(&self) -> String {
        "Created".to_string()
    }
}

/// Environment variable was updated
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpsertDatasetEnvVarResultUpdated {
    /// Updated environment variable
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl UpsertDatasetEnvVarResultUpdated {
    /// Human-readable outcome message
    async fn message(&self) -> String {
        "Updated".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Environment variable key does not match `^[A-Za-z_][A-Za-z0-9_]*$`
#[derive(SimpleObject, Debug)]
pub struct DatasetEnvVarInvalidKey {
    /// Rejected key
    pub env_var_key: String,
    /// Human-readable error message
    pub message: String,
}

impl DatasetEnvVarInvalidKey {
    fn new(env_var_key: String, message: String) -> Self {
        Self {
            env_var_key,
            message,
        }
    }
}

/// Environment variable value is empty
#[derive(SimpleObject, Debug)]
pub struct DatasetEnvVarEmptyValue {
    /// Key of the rejected environment variable
    pub env_var_key: String,
    /// Human-readable error message
    pub message: String,
}

impl DatasetEnvVarEmptyValue {
    fn new(env_var_key: String, message: String) -> Self {
        Self {
            env_var_key,
            message,
        }
    }
}

/// Environment variable value is longer than allowed
#[derive(SimpleObject, Debug)]
pub struct DatasetEnvVarValueTooLong {
    /// Key of the rejected environment variable
    pub env_var_key: String,
    /// Length of the rejected value in bytes
    pub actual_length: usize,
    /// Maximum value length in bytes
    pub max_length: usize,
    /// Human-readable error message
    pub message: String,
}

impl DatasetEnvVarValueTooLong {
    fn new(env_var_key: String, actual_length: usize, max_length: usize, message: String) -> Self {
        Self {
            env_var_key,
            actual_length,
            max_length,
            message,
        }
    }
}

/// Dataset already has the maximum number of variables or of secrets
#[derive(SimpleObject, Debug)]
pub struct DatasetEnvVarTooManyEntries {
    /// Maximum number of variables, or of secrets, per dataset
    pub max_entries: usize,
    /// Human-readable error message
    pub message: String,
}

impl DatasetEnvVarTooManyEntries {
    fn new(max_entries: usize, message: String) -> Self {
        Self {
            max_entries,
            message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outcome of deleting an environment variable
#[derive(Interface, Debug)]
#[graphql(field(
    name = "message",
    ty = "String",
    desc = "Human-readable outcome message"
))]
pub enum DeleteDatasetEnvVarResult {
    Success(DeleteDatasetEnvVarResultSuccess),
    NotFound(DeleteDatasetEnvVarResultNotFound),
}

/// Environment variable was deleted
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultSuccess {
    /// Key of the deleted environment variable
    pub env_var_key: String,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultSuccess {
    /// Human-readable outcome message
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

/// Environment variable with the given key does not exist
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultNotFound {
    /// Key that was not found
    pub env_var_key: String,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultNotFound {
    /// Human-readable error message
    pub async fn message(&self) -> String {
        format!(
            "Environment variable with '{}' key not found",
            self.env_var_key
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
