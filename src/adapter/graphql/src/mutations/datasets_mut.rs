// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{self as domain, DatasetRepositoryExt};
use opendatafabric as odf;

use crate::mutations::DatasetMut;
use crate::prelude::*;
use crate::queries::Dataset;
use crate::LoggedInGuard;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetsMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[Object]
impl DatasetsMut {
    /// Returns a mutable dataset by its ID
    async fn by_id(&self, ctx: &Context<'_>, dataset_id: DatasetID) -> Result<Option<DatasetMut>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let hdl = dataset_repo
            .try_resolve_dataset_ref(&dataset_id.as_local_ref())
            .await?;
        Ok(hdl.map(DatasetMut::new))
    }

    /// Creates a new empty dataset
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_empty(
        &self,
        ctx: &Context<'_>,
        dataset_kind: DatasetKind,
        dataset_alias: DatasetAlias,
    ) -> Result<CreateDatasetResult> {
        match self
            .create_from_snapshot_impl(
                ctx,
                odf::DatasetSnapshot {
                    name: dataset_alias.into(),
                    kind: dataset_kind.into(),
                    metadata: Vec::new(),
                },
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
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_from_snapshot(
        &self,
        ctx: &Context<'_>,
        snapshot: String,
        snapshot_format: MetadataManifestFormat,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        use odf::serde::DatasetSnapshotDeserializer;

        let snapshot = match snapshot_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::serde::yaml::YamlDatasetSnapshotDeserializer;
                match de.read_manifest(snapshot.as_bytes()) {
                    Ok(snapshot) => snapshot,
                    Err(e @ odf::serde::Error::SerdeError { .. }) => {
                        return Ok(CreateDatasetFromSnapshotResult::Malformed(
                            MetadataManifestMalformed {
                                message: e.to_string(),
                            },
                        ));
                    }
                    Err(odf::serde::Error::UnsupportedVersion(e)) => {
                        return Ok(CreateDatasetFromSnapshotResult::UnsupportedVersion(
                            e.into(),
                        ))
                    }
                    Err(e @ odf::serde::Error::IoError { .. }) => return Err(e.int_err().into()),
                }
            }
        };

        self.create_from_snapshot_impl(ctx, snapshot).await
    }

    // TODO: Multi-tenancy
    // TODO: Multi-tenant resolution for derivative dataset inputs (should it only
    //       work by ID?)
    #[allow(unused_variables)]
    #[graphql(skip)]
    async fn create_from_snapshot_impl(
        &self,
        ctx: &Context<'_>,
        snapshot: odf::DatasetSnapshot,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        let create_from_snapshot =
            from_catalog::<dyn domain::CreateDatasetFromSnapshotUseCase>(ctx).unwrap();

        // TODO: read param from input
        let options = Default::default();

        let result = match create_from_snapshot.execute(snapshot, &options).await {
            Ok(result) => {
                let dataset = Dataset::from_ref(ctx, &result.dataset_handle.as_local_ref()).await?;
                CreateDatasetFromSnapshotResult::Success(CreateDatasetResultSuccess { dataset })
            }
            Err(domain::CreateDatasetFromSnapshotError::NameCollision(e)) => {
                CreateDatasetFromSnapshotResult::NameCollision(CreateDatasetResultNameCollision {
                    account_name: e.alias.account_name.map(Into::into),
                    dataset_name: e.alias.dataset_name.into(),
                })
            }
            Err(domain::CreateDatasetFromSnapshotError::RefCollision(e)) => {
                return Err(e.int_err().into())
            }
            Err(domain::CreateDatasetFromSnapshotError::InvalidSnapshot(e)) => {
                CreateDatasetFromSnapshotResult::InvalidSnapshot(
                    CreateDatasetResultInvalidSnapshot { message: e.reason },
                )
            }
            Err(domain::CreateDatasetFromSnapshotError::MissingInputs(e)) => {
                CreateDatasetFromSnapshotResult::MissingInputs(CreateDatasetResultMissingInputs {
                    missing_inputs: e
                        .missing_inputs
                        .into_iter()
                        .map(|r| r.to_string())
                        .collect(),
                })
            }
            Err(domain::CreateDatasetFromSnapshotError::Internal(e)) => return Err(e.into()),
        };

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateDatasetResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateDatasetResult {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision),
}

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateDatasetFromSnapshotResult {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision),
    Malformed(MetadataManifestMalformed),
    UnsupportedVersion(MetadataManifestUnsupportedVersion),
    InvalidSnapshot(CreateDatasetResultInvalidSnapshot),
    // TODO: This error should probably be generalized along with other
    // errors that can occur during the metadata evolution
    MissingInputs(CreateDatasetResultMissingInputs),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
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

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CreateDatasetResultNameCollision {
    pub account_name: Option<AccountName>,
    pub dataset_name: DatasetName,
}

#[ComplexObject]
impl CreateDatasetResultNameCollision {
    async fn message(&self) -> String {
        format!("Dataset with name '{}' already exists", self.dataset_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct CreateDatasetResultInvalidSnapshot {
    pub message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
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
