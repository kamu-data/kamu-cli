// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_dataset_env_vars::{
    DatasetEnvVarService,
    DatasetEnvVarValue,
    DeleteDatasetEnvVarError,
    ModifyDatasetEnvVarError,
    SaveDatasetEnvVarError,
};
use opendatafabric as odf;
use secrecy::Secret;

use crate::prelude::*;
use crate::queries::ViewDatasetEnvVar;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetEnvVarsMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetEnvVarsMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    async fn save_env_variable(
        &self,
        ctx: &Context<'_>,
        key: String,
        value: String,
        is_secret: bool,
    ) -> Result<SaveDatasetEnvVarResult> {
        utils::check_dataset_write_access(ctx, &self.dataset_handle).await?;

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();

        let dataset_env_var_value = if is_secret {
            DatasetEnvVarValue::Secret(Secret::new(value))
        } else {
            DatasetEnvVarValue::Regular(value)
        };

        match dataset_env_var_service
            .create_dataset_env_var(
                key.as_str(),
                &dataset_env_var_value,
                &self.dataset_handle.id,
            )
            .await
        {
            Ok(created_dataset_env_var) => Ok(SaveDatasetEnvVarResult::Success(
                SaveDatasetEnvVarResultSuccess {
                    env_var: ViewDatasetEnvVar::new(created_dataset_env_var),
                },
            )),
            Err(err) => match err {
                SaveDatasetEnvVarError::Duplicate(_) => Ok(SaveDatasetEnvVarResult::Duplicate(
                    SaveDatasetEnvVarResultDuplicate {
                        dataset_env_var_key: key,
                        dataset_name: self.dataset_handle.alias.dataset_name.clone().into(),
                    },
                )),
                SaveDatasetEnvVarError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }

    async fn delete_env_variable(
        &self,
        ctx: &Context<'_>,
        id: DatasetEnvVarID,
    ) -> Result<DeleteDatasetEnvVarResult> {
        utils::check_dataset_write_access(ctx, &self.dataset_handle).await?;

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();

        match dataset_env_var_service
            .delete_dataset_env_var(&id.clone().into())
            .await
        {
            Ok(_) => Ok(DeleteDatasetEnvVarResult::Success(
                DeleteDatasetEnvVarResultSuccess {
                    env_var_id: id.clone(),
                },
            )),
            Err(err) => match err {
                DeleteDatasetEnvVarError::NotFound(_) => Ok(DeleteDatasetEnvVarResult::NotFount(
                    DeleteDatasetEnvVarResultNotFound {
                        env_var_id: id.clone(),
                    },
                )),
                DeleteDatasetEnvVarError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }

    async fn modify_env_variable(
        &self,
        ctx: &Context<'_>,
        id: DatasetEnvVarID,
        new_value: String,
        is_secret: bool,
    ) -> Result<ModifyDatasetEnvVarResult> {
        utils::check_dataset_write_access(ctx, &self.dataset_handle).await?;

        let dataset_env_var_service = from_catalog::<dyn DatasetEnvVarService>(ctx).unwrap();
        let dataset_env_var_value = if is_secret {
            DatasetEnvVarValue::Secret(Secret::new(new_value))
        } else {
            DatasetEnvVarValue::Regular(new_value)
        };

        match dataset_env_var_service
            .modify_dataset_env_var(&id.clone().into(), &dataset_env_var_value)
            .await
        {
            Ok(_) => Ok(ModifyDatasetEnvVarResult::Success(
                ModifyDatasetEnvVarResultSuccess {
                    env_var_id: id.clone(),
                },
            )),
            Err(err) => match err {
                ModifyDatasetEnvVarError::NotFound(_) => Ok(ModifyDatasetEnvVarResult::NotFount(
                    ModifyDatasetEnvVarResultNotFound {
                        env_var_id: id.clone(),
                    },
                )),
                ModifyDatasetEnvVarError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SaveDatasetEnvVarResult {
    Success(SaveDatasetEnvVarResultSuccess),
    Duplicate(SaveDatasetEnvVarResultDuplicate),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SaveDatasetEnvVarResultSuccess {
    pub env_var: ViewDatasetEnvVar,
}

#[ComplexObject]
impl SaveDatasetEnvVarResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SaveDatasetEnvVarResultDuplicate {
    pub dataset_env_var_key: String,
    pub dataset_name: DatasetName,
}

#[ComplexObject]
impl SaveDatasetEnvVarResultDuplicate {
    pub async fn message(&self) -> String {
        format!(
            "Environment variable with {} key for dataset {} already exists",
            self.dataset_env_var_key, self.dataset_name
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteDatasetEnvVarResult {
    Success(DeleteDatasetEnvVarResultSuccess),
    NotFount(DeleteDatasetEnvVarResultNotFound),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultSuccess {
    pub env_var_id: DatasetEnvVarID,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteDatasetEnvVarResultNotFound {
    pub env_var_id: DatasetEnvVarID,
}

#[ComplexObject]
impl DeleteDatasetEnvVarResultNotFound {
    pub async fn message(&self) -> String {
        format!("Environment variable with {} id not found", self.env_var_id,)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum ModifyDatasetEnvVarResult {
    Success(ModifyDatasetEnvVarResultSuccess),
    NotFount(ModifyDatasetEnvVarResultNotFound),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ModifyDatasetEnvVarResultSuccess {
    pub env_var_id: DatasetEnvVarID,
}

#[ComplexObject]
impl ModifyDatasetEnvVarResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct ModifyDatasetEnvVarResultNotFound {
    pub env_var_id: DatasetEnvVarID,
}

#[ComplexObject]
impl ModifyDatasetEnvVarResultNotFound {
    pub async fn message(&self) -> String {
        format!("Environment variable with {} id not found", self.env_var_id,)
    }
}
