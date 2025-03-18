// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{
    DatasetEnvVarService,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
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

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl<'a> DatasetEnvVarsMut<'a> {
    #[graphql(skip)]
    pub async fn new_with_access_check(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        utils::ensure_dataset_env_vars_enabled(ctx)?;
        // TODO: Private Datasets: use check_dataset_maintain_access()
        utils::check_dataset_write_access(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    #[tracing::instrument(level = "info", name = DatasetEnvVarsMut_upsert_env_variable, skip_all)]
    async fn upsert_env_variable(
        &self,
        ctx: &Context<'_>,
        key: String,
        value: String,
        is_secret: bool,
    ) -> Result<UpsertDatasetEnvVarResult> {
        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);

        let dataset_env_var_value = if is_secret {
            DatasetEnvVarValue::Secret(SecretString::from(value))
        } else {
            DatasetEnvVarValue::Regular(value)
        };

        let upsert_result = dataset_env_var_service
            .upsert_dataset_env_var(
                key.as_str(),
                &dataset_env_var_value,
                self.dataset_request_state.dataset_id(),
            )
            .await?;

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

    #[tracing::instrument(level = "info", name = DatasetEnvVarsMut_delete_env_variable, skip_all)]
    async fn delete_env_variable(
        &self,
        ctx: &Context<'_>,
        id: DatasetEnvVarID<'static>,
    ) -> Result<DeleteDatasetEnvVarResult> {
        let dataset_env_var_service = from_catalog_n!(ctx, dyn DatasetEnvVarService);

        match dataset_env_var_service.delete_dataset_env_var(&id).await {
            Ok(_) => Ok(DeleteDatasetEnvVarResult::Success(
                DeleteDatasetEnvVarResultSuccess { env_var_id: id },
            )),
            Err(err) => match err {
                DeleteDatasetEnvVarError::NotFound(_) => Ok(DeleteDatasetEnvVarResult::NotFound(
                    DeleteDatasetEnvVarResultNotFound { env_var_id: id },
                )),
                DeleteDatasetEnvVarError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum UpsertDatasetEnvVarResult {
    Created(UpsertDatasetEnvVarResultCreated),
    Updated(UpsertDatasetEnvVarResultUpdated),
    UpToDate(UpsertDatasetEnvVarUpToDate),
}

#[derive(Debug)]
pub struct UpsertDatasetEnvVarUpToDate;

#[Object]
impl UpsertDatasetEnvVarUpToDate {
    pub async fn message(&self) -> String {
        "Dataset env var is up to date".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpsertDatasetEnvVarResultCreated {
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl UpsertDatasetEnvVarResultCreated {
    async fn message(&self) -> String {
        "Created".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct UpsertDatasetEnvVarResultUpdated {
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl UpsertDatasetEnvVarResultUpdated {
    async fn message(&self) -> String {
        "Updated".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct SaveDatasetEnvVarResultDuplicate<'a> {
    pub dataset_env_var_key: String,
    pub dataset_name: DatasetName<'a>,
}

#[ComplexObject]
impl SaveDatasetEnvVarResultDuplicate<'_> {
    pub async fn message(&self) -> String {
        format!(
            "Environment variable with {} key for dataset {} already exists",
            self.dataset_env_var_key, self.dataset_name
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteDatasetEnvVarResult<'a> {
    Success(DeleteDatasetEnvVarResultSuccess<'a>),
    NotFound(DeleteDatasetEnvVarResultNotFound<'a>),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultSuccess<'a> {
    pub env_var_id: DatasetEnvVarID<'a>,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultSuccess<'_> {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultNotFound<'a> {
    pub env_var_id: DatasetEnvVarID<'a>,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultNotFound<'_> {
    pub async fn message(&self) -> String {
        format!("Environment variable with {} id not found", self.env_var_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ModifyDatasetEnvVarResult<'a> {
    Success(ModifyDatasetEnvVarResultSuccess<'a>),
    NotFound(ModifyDatasetEnvVarResultNotFound<'a>),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ModifyDatasetEnvVarResultSuccess<'a> {
    pub env_var_id: DatasetEnvVarID<'a>,
}

#[ComplexObject]
impl ModifyDatasetEnvVarResultSuccess<'_> {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct ModifyDatasetEnvVarResultNotFound<'a> {
    pub env_var_id: DatasetEnvVarID<'a>,
}

#[ComplexObject]
impl ModifyDatasetEnvVarResultNotFound<'_> {
    pub async fn message(&self) -> String {
        format!("Environment variable with {} id not found", self.env_var_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
