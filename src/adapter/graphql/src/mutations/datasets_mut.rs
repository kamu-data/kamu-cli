// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::auth;
use kamu_core::auth::DatasetActionAccess;
use kamu_datasets::CreateDatasetFromSnapshotError;

use crate::mutations::DatasetMut;
use crate::prelude::*;
use crate::queries::{Account, Dataset, DatasetRequestState};
use crate::{utils, LoggedInGuard};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetsMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl DatasetsMut {
    /// Returns a mutable dataset by its ID
    #[tracing::instrument(level = "info", name = DatasetsMut_by_id, skip_all, fields(%dataset_id))]
    async fn by_id(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID<'_>,
    ) -> Result<Option<DatasetMut>> {
        let dataset_registry = from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);

        use kamu_core::DatasetRegistryExt;

        let maybe_dataset_handle = dataset_registry
            .try_resolve_dataset_handle_by_ref(&dataset_id.as_local_ref())
            .await?;
        let Some(dataset_handle) = maybe_dataset_handle else {
            return Ok(None);
        };

        let dataset_request_state = DatasetRequestState::new(dataset_handle);
        let allowed_actions = dataset_request_state.allowed_dataset_actions(ctx).await?;
        let current_dataset_action = auth::DatasetAction::Write;

        match auth::DatasetAction::resolve_access(allowed_actions, current_dataset_action) {
            DatasetActionAccess::Full => {
                Ok(Some(DatasetMut::new_access_checked(dataset_request_state)))
            }
            DatasetActionAccess::Limited => Err(utils::make_dataset_access_error(
                dataset_request_state.dataset_handle(),
            )),
            DatasetActionAccess::Forbidden => Ok(None),
        }
    }

    /// Creates a new empty dataset
    #[tracing::instrument(level = "info", name = DatasetsMut_create_empty, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_empty(
        &self,
        ctx: &Context<'_>,
        dataset_kind: DatasetKind,
        dataset_alias: DatasetAlias<'_>,
        dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetResult> {
        match self
            .create_from_snapshot_impl(
                ctx,
                odf::DatasetSnapshot {
                    name: dataset_alias.into(),
                    kind: dataset_kind.into(),
                    metadata: Vec::new(),
                },
                dataset_visibility.into(),
            )
            .await?
        {
            CreateDatasetFromSnapshotResult::Success(s) => Ok(CreateDatasetResult::Success(s)),
            CreateDatasetFromSnapshotResult::NameCollision(e) => {
                Ok(CreateDatasetResult::NameCollision(e))
            }
            CreateDatasetFromSnapshotResult::InvalidSnapshot(_)
            | CreateDatasetFromSnapshotResult::Malformed(_)
            | CreateDatasetFromSnapshotResult::UnsupportedVersion(_)
            | CreateDatasetFromSnapshotResult::MissingInputs(_) => unreachable!(),
        }
    }

    /// Creates a new dataset from provided DatasetSnapshot manifest
    #[tracing::instrument(level = "info", name = DatasetsMut_create_from_snapshot, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_from_snapshot(
        &self,
        ctx: &Context<'_>,
        snapshot: String,
        snapshot_format: MetadataManifestFormat,
        dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        use odf::metadata::serde::DatasetSnapshotDeserializer;

        let snapshot = match snapshot_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::metadata::serde::yaml::YamlDatasetSnapshotDeserializer;
                match de.read_manifest(snapshot.as_bytes()) {
                    Ok(snapshot) => snapshot,
                    Err(e @ odf::metadata::serde::Error::SerdeError { .. }) => {
                        return Ok(CreateDatasetFromSnapshotResult::Malformed(
                            MetadataManifestMalformed {
                                message: e.to_string(),
                            },
                        ));
                    }
                    Err(odf::metadata::serde::Error::UnsupportedVersion(e)) => {
                        return Ok(CreateDatasetFromSnapshotResult::UnsupportedVersion(
                            e.into(),
                        ))
                    }
                    Err(e @ odf::metadata::serde::Error::IoError { .. }) => {
                        return Err(e.int_err().into())
                    }
                }
            }
        };

        self.create_from_snapshot_impl(
            ctx,
            snapshot,
            dataset_visibility.map(Into::into).unwrap_or_default(),
        )
        .await
    }

    // TODO: Multi-tenancy
    //       https://github.com/kamu-data/kamu-cli/issues/891

    // TODO: Multi-tenant resolution for derivative dataset inputs (should it only
    //       work by ID?)
    #[graphql(skip)]
    async fn create_from_snapshot_impl(
        &self,
        ctx: &Context<'_>,
        snapshot: odf::DatasetSnapshot,
        dataset_visibility: odf::DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        let create_from_snapshot =
            from_catalog_n!(ctx, dyn kamu_datasets::CreateDatasetFromSnapshotUseCase);

        let create_options = kamu_datasets::CreateDatasetUseCaseOptions { dataset_visibility };

        let result = match create_from_snapshot.execute(snapshot, create_options).await {
            Ok(result) => {
                let account = Account::from_dataset_alias(ctx, &result.dataset_handle.alias)
                    .await?
                    .expect("Account must exist");
                let dataset = Dataset::from_resolved_authorized_dataset(
                    account,
                    &kamu_core::ResolvedDataset::from_created(&result),
                );
                CreateDatasetFromSnapshotResult::Success(CreateDatasetResultSuccess { dataset })
            }
            Err(CreateDatasetFromSnapshotError::NameCollision(e)) => {
                CreateDatasetFromSnapshotResult::NameCollision(CreateDatasetResultNameCollision {
                    account_name: e.alias.account_name.map(Into::into),
                    dataset_name: e.alias.dataset_name.into(),
                })
            }
            Err(CreateDatasetFromSnapshotError::RefCollision(e)) => return Err(e.int_err().into()),
            Err(CreateDatasetFromSnapshotError::InvalidSnapshot(e)) => {
                CreateDatasetFromSnapshotResult::InvalidSnapshot(
                    CreateDatasetResultInvalidSnapshot { message: e.reason },
                )
            }
            Err(CreateDatasetFromSnapshotError::MissingInputs(e)) => {
                CreateDatasetFromSnapshotResult::MissingInputs(CreateDatasetResultMissingInputs {
                    missing_inputs: e
                        .missing_inputs
                        .into_iter()
                        .map(|r| r.to_string())
                        .collect(),
                })
            }
            Err(CreateDatasetFromSnapshotError::CASFailed(e)) => return Err(e.int_err().into()),
            Err(CreateDatasetFromSnapshotError::Internal(e)) => return Err(e.into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateDatasetResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateDatasetResult<'a> {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision<'a>),
}

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateDatasetFromSnapshotResult<'a> {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision<'a>),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    InvalidSnapshot(CreateDatasetResultInvalidSnapshot),
    // TODO: This error should probably be generalized along with other
    // errors that can occur during the metadata evolution
    MissingInputs(CreateDatasetResultMissingInputs),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultSuccess {
    pub dataset: Dataset,
}

#[ComplexObject]
impl CreateDatasetResultSuccess {
    async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultNameCollision<'a> {
    pub account_name: Option<AccountName<'a>>,
    pub dataset_name: DatasetName<'a>,
}

#[ComplexObject]
impl CreateDatasetResultNameCollision<'_> {
    async fn message(&self) -> String {
        format!("Dataset with name '{}' already exists", self.dataset_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub struct CreateDatasetResultInvalidSnapshot {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultMissingInputs {
    // TODO: Input can be referenced by ID or by name
    // do we need DatasetRef-style scalar for GQL?
    pub missing_inputs: Vec<String>,
}

#[ComplexObject]
impl CreateDatasetResultMissingInputs {
    async fn message(&self) -> String {
        format!(
            "Dataset is referencing non-existing inputs: {}",
            self.missing_inputs.join(", ")
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
