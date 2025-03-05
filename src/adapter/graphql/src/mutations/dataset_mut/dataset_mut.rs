// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_core::{self as domain, SetWatermarkPlanningError, SetWatermarkUseCase};
use kamu_datasets::{DeleteDatasetError, RenameDatasetError};

use crate::mutations::{
    ensure_account_is_owner_or_admin,
    DatasetEnvVarsMut,
    DatasetFlowsMut,
    DatasetMetadataMut,
};
use crate::prelude::*;
use crate::queries::*;
use crate::utils::{ensure_dataset_env_vars_enabled, from_catalog_n};
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetMut {
    dataset_handle: odf::DatasetHandle,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
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
    #[tracing::instrument(level = "info", name = DatasetMut_rename, skip_all)]
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

        let rename_dataset = from_catalog_n!(ctx, dyn kamu_datasets::RenameDatasetUseCase);
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
    #[tracing::instrument(level = "info", name = DatasetMut_delete, skip_all)]
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteResult> {
        let delete_dataset = from_catalog_n!(ctx, dyn kamu_datasets::DeleteDatasetUseCase);
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
    #[tracing::instrument(level = "info", name = DatasetMut_set_watermark, skip_all)]
    async fn set_watermark(
        &self,
        ctx: &Context<'_>,
        watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult> {
        let set_watermark_use_case = from_catalog_n!(ctx, dyn SetWatermarkUseCase);
        match set_watermark_use_case
            .execute(&self.dataset_handle, watermark)
            .await
        {
            Ok(domain::SetWatermarkResult::UpToDate) => {
                Ok(SetWatermarkResult::UpToDate(SetWatermarkUpToDate {
                    _dummy: String::new(),
                }))
            }
            Ok(domain::SetWatermarkResult::Updated { new_head, .. }) => {
                Ok(SetWatermarkResult::Updated(SetWatermarkUpdated {
                    new_head: new_head.into(),
                }))
            }
            Err(
                e @ domain::SetWatermarkError::Planning(SetWatermarkPlanningError::IsDerivative),
            ) => Ok(SetWatermarkResult::IsDerivative(SetWatermarkIsDerivative {
                message: e.to_string(),
            })),
            Err(e) => Err(e.int_err().into()),
        }
    }

    /// Set visibility for the dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    #[tracing::instrument(level = "info", name = DatasetMut_set_visibility, skip_all)]
    async fn set_visibility(
        &self,
        ctx: &Context<'_>,
        visibility: DatasetVisibilityInput,
    ) -> Result<SetDatasetVisibilityResult> {
        ensure_account_is_owner_or_admin(ctx, &self.dataset_handle).await?;

        let rebac_svc = from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacService);

        let (allows_public_read, allows_anonymous_read) = match visibility {
            DatasetVisibilityInput::Private(_) => (false, false),
            DatasetVisibilityInput::Public(PublicDatasetVisibility {
                anonymous_available,
            }) => (true, anonymous_available),
        };

        use kamu_auth_rebac::DatasetPropertyName;

        for (name, value) in [
            DatasetPropertyName::allows_public_read(allows_public_read),
            DatasetPropertyName::allows_anonymous_read(allows_anonymous_read),
        ] {
            rebac_svc
                .set_dataset_property(&self.dataset_handle.id, name, &value)
                .await
                .int_err()?;
        }

        Ok(SetDatasetVisibilityResultSuccess::default().into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RenameResult {
    Success(RenameResultSuccess),
    NoChanges(RenameResultNoChanges),
    NameCollision(RenameResultNameCollision),
}

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
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

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteResult {
    Success(DeleteResultSuccess),
    DanglingReference(DeleteResultDanglingReference),
}

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
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

#[derive(OneofObject)]
pub enum DatasetVisibilityInput {
    Private(PrivateDatasetVisibility),
    Public(PublicDatasetVisibility),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetDatasetVisibilityResult {
    Success(SetDatasetVisibilityResultSuccess),
}

#[derive(SimpleObject, Debug, Default)]
#[graphql(complex)]
pub struct SetDatasetVisibilityResultSuccess {
    _dummy: Option<String>,
}

#[ComplexObject]
impl SetDatasetVisibilityResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetWatermarkResult {
    UpToDate(SetWatermarkUpToDate),
    Updated(SetWatermarkUpdated),
    IsDerivative(SetWatermarkIsDerivative),
}

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
pub struct SetWatermarkIsDerivative {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
