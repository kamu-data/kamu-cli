// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRegistryExt;
use kamu_datasets::CreateDatasetFromSnapshotError;

use crate::mutations::DatasetMut;
use crate::prelude::*;
use crate::queries::Dataset;
use crate::LoggedInGuard;

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
        let hdl = dataset_registry
            .try_resolve_dataset_handle_by_ref(&dataset_id.as_local_ref())
            .await?;
        Ok(hdl.map(DatasetMut::new))
    }

    /// Creates a new empty dataset
    #[tracing::instrument(level = "info", name = DatasetsMut_create_empty, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_empty(
        &self,
        ctx: &Context<'_>,
        dataset_kind: DatasetKind,
        dataset_alias: DatasetAlias<'_>,
        // TODO: Private Datasets: GQL: make new parameters mandatory, after frontend update
        //       https://github.com/kamu-data/kamu-cli/issues/780
        dataset_visibility: Option<DatasetVisibility>,
    ) -> Result<CreateDatasetResult> {
        match self
            .create_from_snapshot_impl(
                ctx,
                odf::DatasetSnapshot {
                    name: dataset_alias.into(),
                    kind: dataset_kind.into(),
                    metadata: Vec::new(),
                },
                dataset_visibility.map(Into::into).unwrap_or_default(),
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
        // TODO: Private Datasets: GQL: make new parameters mandatory, after frontend update
        //       https://github.com/kamu-data/kamu-cli/issues/780
        dataset_visibility: Option<DatasetVisibility>,
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
                let dataset = Dataset::from_ref(ctx, &result.dataset_handle.as_local_ref()).await?;
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
