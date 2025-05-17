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

use crate::mutations::*;
use crate::prelude::*;
use crate::queries::*;
use crate::utils::{self, from_catalog_n};
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetMut {
    dataset_request_state: DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl DatasetMut {
    #[graphql(skip)]
    pub fn new_access_checked(dataset_request_state: DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Access to the mutable metadata of the dataset
    async fn metadata(&self) -> DatasetMetadataMut {
        DatasetMetadataMut::new(&self.dataset_request_state)
    }

    /// Access to the mutable flow configurations of this dataset
    async fn flows(&self, ctx: &Context<'_>) -> Result<DatasetFlowsMut> {
        DatasetFlowsMut::new_with_access_check(ctx, &self.dataset_request_state).await
    }

    /// Access to the mutable flow configurations of this dataset
    async fn env_vars(&self, ctx: &Context<'_>) -> Result<DatasetEnvVarsMut> {
        DatasetEnvVarsMut::new_with_access_check(ctx, &self.dataset_request_state).await
    }

    /// Access to collaboration management methods
    async fn collaboration(&self, ctx: &Context<'_>) -> Result<DatasetCollaborationMut> {
        DatasetCollaborationMut::new_with_access_check(ctx, &self.dataset_request_state).await
    }

    /// Access to webhooks subscriptions management methods
    async fn webhook_subscriptions(
        &self,
        ctx: &Context<'_>,
    ) -> Result<DatasetWebhookSubscriptionsMut> {
        DatasetWebhookSubscriptionsMut::new_with_access_check(ctx, &self.dataset_request_state)
            .await
    }

    /// Rename the dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    #[tracing::instrument(level = "info", name = DatasetMut_rename, skip_all)]
    async fn rename(
        &self,
        ctx: &Context<'_>,
        new_name: DatasetName<'static>,
    ) -> Result<RenameResult<'_>> {
        // NOTE: Access verification is handled by the use-case

        let dataset_handle = self.dataset_request_state.dataset_handle();

        if dataset_handle.alias.dataset_name.as_str() == new_name.as_str() {
            return Ok(RenameResult::NoChanges(RenameResultNoChanges {
                preserved_name: new_name,
            }));
        }

        let rename_dataset_use_case = from_catalog_n!(ctx, dyn kamu_datasets::RenameDatasetUseCase);

        match rename_dataset_use_case
            .execute(&dataset_handle.as_local_ref(), &new_name)
            .await
        {
            Ok(_) => Ok(RenameResult::Success(RenameResultSuccess {
                old_name: (&dataset_handle.alias.dataset_name).into(),
                new_name,
            })),
            Err(RenameDatasetError::NameCollision(e)) => {
                Ok(RenameResult::NameCollision(RenameResultNameCollision {
                    colliding_alias: e.alias.into(),
                }))
            }
            Err(RenameDatasetError::Access(_)) => {
                Err(utils::make_dataset_access_error(dataset_handle))
            }
            // "Not found" should not be reachable, since we've just resolved the dataset by ID
            Err(RenameDatasetError::NotFound(e)) => Err(e.int_err().into()),
            Err(RenameDatasetError::Internal(e)) => Err(e.into()),
        }
    }

    /// Delete the dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    #[tracing::instrument(level = "info", name = DatasetMut_delete, skip_all)]
    async fn delete(&self, ctx: &Context<'_>) -> Result<DeleteResult> {
        // NOTE: Access verification is handled by the use-case

        let delete_dataset_use_case = from_catalog_n!(ctx, dyn kamu_datasets::DeleteDatasetUseCase);

        let dataset_handle = self.dataset_request_state.dataset_handle();

        match delete_dataset_use_case
            .execute_via_handle(dataset_handle)
            .await
        {
            Ok(_) => Ok(DeleteResult::Success(DeleteResultSuccess {
                deleted_dataset: (&dataset_handle.alias).into(),
            })),
            Err(DeleteDatasetError::DanglingReference(e)) => Ok(DeleteResult::DanglingReference(
                DeleteResultDanglingReference {
                    not_deleted_dataset: (&dataset_handle.alias).into(),
                    dangling_child_refs: e
                        .children
                        .iter()
                        .map(|child_dataset| child_dataset.as_local_ref().into())
                        .collect(),
                },
            )),
            Err(DeleteDatasetError::Access(_)) => {
                Err(utils::make_dataset_access_error(dataset_handle))
            }
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
        // NOTE: Access verification is handled by the use-case

        let set_watermark_use_case = from_catalog_n!(ctx, dyn SetWatermarkUseCase);

        match set_watermark_use_case
            .execute(self.dataset_request_state.dataset_handle(), watermark)
            .await
        {
            Ok(domain::SetWatermarkResult::UpToDate) => {
                Ok(SetWatermarkResult::UpToDate(SetWatermarkUpToDate::default()))
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
    #[tracing::instrument(level = "info", name = DatasetMut_set_visibility, skip_all)]
    async fn set_visibility(
        &self,
        ctx: &Context<'_>,
        visibility: DatasetVisibilityInput,
    ) -> Result<SetDatasetVisibilityResult> {
        utils::check_dataset_maintain_access(ctx, &self.dataset_request_state).await?;

        let rebac_svc = from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacService);

        let (allows_public_read, allows_anonymous_read) = match visibility {
            DatasetVisibilityInput::Private(_) => (false, false),
            DatasetVisibilityInput::Public(PublicDatasetVisibility {
                anonymous_available,
            }) => (true, anonymous_available),
        };

        use kamu_auth_rebac::DatasetPropertyName;

        let dataset_id = self.dataset_request_state.dataset_id();

        for (name, value) in [
            DatasetPropertyName::allows_public_read(allows_public_read),
            DatasetPropertyName::allows_anonymous_read(allows_anonymous_read),
        ] {
            rebac_svc
                .set_dataset_property(dataset_id, name, &value)
                .await
                .int_err()?;
        }

        Ok(SetDatasetVisibilityResultSuccess::default().into())
    }

    /// Downcast a dataset to a versioned file interface
    async fn as_versioned_file(&self, ctx: &Context<'_>) -> Result<Option<VersionedFileMut>> {
        Ok(Some(VersionedFileMut::new(
            self.dataset_request_state
                .resolved_dataset(ctx)
                .await?
                .clone(),
        )))
    }

    /// Downcast a dataset to a collection interface
    async fn as_collection(&self, ctx: &Context<'_>) -> Result<Option<CollectionMut>> {
        Ok(Some(CollectionMut::new(
            self.dataset_request_state
                .resolved_dataset(ctx)
                .await?
                .clone(),
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RenameResult<'a> {
    Success(RenameResultSuccess<'a>),
    NoChanges(RenameResultNoChanges<'a>),
    NameCollision(RenameResultNameCollision<'a>),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct RenameResultSuccess<'a> {
    pub old_name: DatasetName<'a>,
    pub new_name: DatasetName<'a>,
}

#[ComplexObject]
impl RenameResultSuccess<'_> {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct RenameResultNoChanges<'a> {
    pub preserved_name: DatasetName<'a>,
}

#[ComplexObject]
impl RenameResultNoChanges<'_> {
    async fn message(&self) -> String {
        "No changes".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct RenameResultNameCollision<'a> {
    pub colliding_alias: DatasetAlias<'a>,
}

#[ComplexObject]
impl RenameResultNameCollision<'_> {
    async fn message(&self) -> String {
        format!("Dataset '{}' already exists", self.colliding_alias)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum DeleteResult<'a> {
    Success(DeleteResultSuccess<'a>),
    DanglingReference(DeleteResultDanglingReference<'a>),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteResultSuccess<'a> {
    pub deleted_dataset: DatasetAlias<'a>,
}

#[ComplexObject]
impl DeleteResultSuccess<'_> {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct DeleteResultDanglingReference<'a> {
    pub not_deleted_dataset: DatasetAlias<'a>,
    pub dangling_child_refs: Vec<DatasetRef<'a>>,
}

#[ComplexObject]
impl DeleteResultDanglingReference<'_> {
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

#[derive(SimpleObject, Debug)]
pub struct SetDatasetVisibilityResultSuccess {
    message: String,
}

impl Default for SetDatasetVisibilityResultSuccess {
    fn default() -> Self {
        Self {
            message: "Success".to_string(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum SetWatermarkResult<'a> {
    UpToDate(SetWatermarkUpToDate),
    Updated(SetWatermarkUpdated<'a>),
    IsDerivative(SetWatermarkIsDerivative),
}

#[derive(SimpleObject, Debug)]
pub struct SetWatermarkUpToDate {
    message: String,
}

impl Default for SetWatermarkUpToDate {
    fn default() -> Self {
        Self {
            message: "UpToDate".to_string(),
        }
    }
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct SetWatermarkUpdated<'a> {
    pub new_head: Multihash<'a>,
}

#[ComplexObject]
impl SetWatermarkUpdated<'_> {
    async fn message(&self) -> String {
        "Updated".to_string()
    }
}

#[derive(SimpleObject, Debug)]
pub struct SetWatermarkIsDerivative {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
