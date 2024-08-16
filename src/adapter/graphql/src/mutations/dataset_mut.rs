// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use domain::{DeleteDatasetError, RenameDatasetError};
use kamu_core::{self as domain};
use opendatafabric as odf;

use super::{DatasetEnvVarsMut, DatasetFlowsMut, DatasetMetadataMut};
use crate::prelude::*;
use crate::utils::ensure_dataset_env_vars_enabled;
use crate::LoggedInGuard;

#[derive(Debug, Clone)]
pub struct DatasetMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Access to the mutable metadata of the dataset
    async fn metadata(&self) -> DatasetMetadataMut {
        DatasetMetadataMut::new(self.dataset_handle.clone())
    }

    /// Access to the mutable flow configurations of this dataset
    async fn flows(&self) -> DatasetFlowsMut {
        DatasetFlowsMut::new(self.dataset_handle.clone())
    }

    /// Access to the mutable flow configurations of this dataset
    #[allow(clippy::unused_async)]
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVarsMut> {
        ensure_dataset_env_vars_enabled(ctx)?;

        Ok(DatasetEnvVarsMut::new(self.dataset_handle.clone()))
    }

    /// Rename the dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn rename(&self, ctx: &Context<'_>, new_name: DatasetName) -> Result<RenameResult> {
        if self
            .dataset_handle
            .alias
            .dataset_name
            .as_str()
            .eq(new_name.as_str())
        {
            return Ok(RenameResult::NoChanges(RenameResultNoChanges {
                preserved_name: new_name,
            }));
        }

        let rename_dataset = from_catalog::<dyn domain::RenameDatasetUseCase>(ctx).unwrap();
        match rename_dataset
            .execute(&self.dataset_handle.as_local_ref(), &new_name)
            .await
        {
            Ok(_) => Ok(RenameResult::Success(RenameResultSuccess {
                old_name: self.dataset_handle.alias.dataset_name.clone().into(),
                new_name,
            })),
            Err(RenameDatasetError::NameCollision(e)) => {
                Ok(RenameResult::NameCollision(RenameResultNameCollision {
                    colliding_alias: e.alias.into(),
                }))
            }
            Err(RenameDatasetError::Access(_)) => Err(GqlError::Gql(
                Error::new("Dataset access error")
                    .extend_with(|_, eev| eev.set("alias", self.dataset_handle.alias.to_string())),
            )),
            // "Not found" should not be reachable, since we've just resolved the dataset by ID
            Err(RenameDatasetError::NotFound(e)) => Err(e.int_err().into()),
            Err(RenameDatasetError::Internal(e)) => Err(e.into()),
        }
    }

    /// Delete the dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteResult> {
        let delete_dataset = from_catalog::<dyn domain::DeleteDatasetUseCase>(ctx).unwrap();
        match delete_dataset
            .execute_via_handle(&self.dataset_handle)
            .await
        {
            Ok(_) => Ok(DeleteResult::Success(DeleteResultSuccess {
                deleted_dataset: self.dataset_handle.alias.clone().into(),
            })),
            Err(DeleteDatasetError::DanglingReference(e)) => Ok(DeleteResult::DanglingReference(
                DeleteResultDanglingReference {
                    not_deleted_dataset: self.dataset_handle.alias.clone().into(),
                    dangling_child_refs: e
                        .children
                        .iter()
                        .map(|child_dataset| child_dataset.as_local_ref().into())
                        .collect(),
                },
            )),
            Err(DeleteDatasetError::Access(_)) => Err(GqlError::Gql(
                Error::new("Dataset access error")
                    .extend_with(|_, eev| eev.set("alias", self.dataset_handle.alias.to_string())),
            )),
            // "Not found" should not be reachable, since we've just resolved the dataset by ID
            Err(DeleteDatasetError::NotFound(e)) => Err(e.int_err().into()),
            Err(DeleteDatasetError::Internal(e)) => Err(e.into()),
        }
    }

    /// Manually advances the watermark of a root dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn set_watermark(
        &self,
        ctx: &Context<'_>,
        watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult> {
        let pull_svc = from_catalog::<dyn domain::PullService>(ctx).unwrap();
        match pull_svc
            .set_watermark(&self.dataset_handle.as_local_ref(), watermark)
            .await
        {
            Ok(domain::PullResult::UpToDate(_)) => {
                Ok(SetWatermarkResult::UpToDate(SetWatermarkUpToDate {
                    _dummy: String::new(),
                }))
            }
            Ok(domain::PullResult::Updated { new_head, .. }) => {
                Ok(SetWatermarkResult::Updated(SetWatermarkUpdated {
                    new_head: new_head.into(),
                }))
            }
            Err(e @ domain::SetWatermarkError::IsDerivative) => {
                Ok(SetWatermarkResult::IsDerivative(SetWatermarkIsDerivative {
                    message: e.to_string(),
                }))
            }
            Err(e) => Err(e.int_err().into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RenameResult {
    Success(RenameResultSuccess),
    NoChanges(RenameResultNoChanges),
    NameCollision(RenameResultNameCollision),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultSuccess {
    pub old_name: DatasetName,
    pub new_name: DatasetName,
}

#[ComplexObject]
impl RenameResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultNoChanges {
    pub preserved_name: DatasetName,
}

#[ComplexObject]
impl RenameResultNoChanges {
    async fn message(&self) -> String {
        "No changes".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct RenameResultNameCollision {
    pub colliding_alias: DatasetAlias,
}

#[ComplexObject]
impl RenameResultNameCollision {
    async fn message(&self) -> String {
        format!("Dataset '{}' already exists", self.colliding_alias)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteResult {
    Success(DeleteResultSuccess),
    DanglingReference(DeleteResultDanglingReference),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteResultSuccess {
    pub deleted_dataset: DatasetAlias,
}

#[ComplexObject]
impl DeleteResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct DeleteResultDanglingReference {
    pub not_deleted_dataset: DatasetAlias,
    pub dangling_child_refs: Vec<DatasetRef>,
}

#[ComplexObject]
impl DeleteResultDanglingReference {
    async fn message(&self) -> String {
        format!(
            "Dataset '{}' has {} dangling reference(s)",
            self.not_deleted_dataset,
            self.dangling_child_refs.len()
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetWatermarkResult {
    UpToDate(SetWatermarkUpToDate),
    Updated(SetWatermarkUpdated),
    IsDerivative(SetWatermarkIsDerivative),
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SetWatermarkUpToDate {
    pub _dummy: String,
}

#[ComplexObject]
impl SetWatermarkUpToDate {
    async fn message(&self) -> String {
        "UpToDate".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct SetWatermarkUpdated {
    pub new_head: Multihash,
}

#[ComplexObject]
impl SetWatermarkUpdated {
    async fn message(&self) -> String {
        "Updated".to_string()
    }
}

#[derive(SimpleObject, Debug, Clone)]
pub struct SetWatermarkIsDerivative {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
