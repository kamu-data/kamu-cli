// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use futures::TryStreamExt;
use kamu::domain;
use kamu::domain::DatasetRepositoryExt;
use opendatafabric as odf;

///////////////////////////////////////////////////////////////////////////////

pub struct Datasets;

#[Object]
impl Datasets {
    const DEFAULT_PER_PAGE: usize = 15;

    /// Returns dataset by its ID
    async fn by_id(&self, ctx: &Context<'_>, dataset_id: DatasetID) -> Result<Option<Dataset>> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let hdl = local_repo
            .try_resolve_dataset_ref(&dataset_id.as_local_ref())
            .await?;
        Ok(hdl.map(|h| Dataset::new(Account::mock(), h)))
    }

    // TODO: Multitenancy
    /// Returns dataset by its owner and name
    #[allow(unused_variables)]
    async fn by_owner_and_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
        dataset_name: DatasetName,
    ) -> Result<Option<Dataset>> {
        let account = Account::mock();
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let hdl = local_repo
            .try_resolve_dataset_ref(&dataset_name.as_local_ref())
            .await?;
        Ok(hdl.map(|h| Dataset::new(Account::mock(), h)))
    }

    // TODO: Multitenancy
    #[graphql(skip)]
    async fn by_account_impl(
        &self,
        ctx: &Context<'_>,
        account: Account,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        let page = page.unwrap_or(0);
        let per_page = per_page.unwrap_or(Self::DEFAULT_PER_PAGE);

        let mut all_datasets: Vec<_> = local_repo.get_all_datasets().try_collect().await?;
        let total_count = all_datasets.len();
        all_datasets.sort_by(|a, b| a.name.cmp(&b.name));

        let nodes = all_datasets
            .into_iter()
            .skip(page * per_page)
            .take(per_page)
            .map(|hdl| Dataset::new(account.clone(), hdl))
            .collect();

        Ok(DatasetConnection::new(nodes, page, per_page, total_count))
    }

    /// Returns datasets belonging to the specified account
    #[allow(unused_variables)]
    async fn by_account_id(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let account = Account::mock();
        self.by_account_impl(ctx, account, page, per_page).await
    }

    /// Returns datasets belonging to the specified account
    #[allow(unused_variables)]
    async fn by_account_name(
        &self,
        ctx: &Context<'_>,
        account_name: AccountName,
        page: Option<usize>,
        per_page: Option<usize>,
    ) -> Result<DatasetConnection> {
        let account = Account::mock();
        self.by_account_impl(ctx, account, page, per_page).await
    }

    /// Creates a new empty dataset
    async fn create_empty(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        dataset_kind: DatasetKind,
        dataset_name: DatasetName,
    ) -> Result<CreateDatasetResult> {
        match self
            .create_from_snapshot_impl(
                ctx,
                account_id,
                odf::DatasetSnapshot {
                    name: dataset_name.into(),
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
    async fn create_from_snapshot(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
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
                    Err(e @ odf::serde::Error::IoError { .. }) => return Err(e.into()),
                }
            }
        };

        self.create_from_snapshot_impl(ctx, account_id, snapshot)
            .await
    }

    // TODO: Multitenancy
    // TODO: Multitenant resolution for derivative dataset inputs (should it only work by ID?)
    #[allow(unused_variables)]
    #[graphql(skip)]
    async fn create_from_snapshot_impl(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        snapshot: odf::DatasetSnapshot,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        let local_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        let result = match domain::create_dataset_from_snapshot(local_repo.as_ref(), snapshot).await
        {
            Ok(result) => {
                let dataset = Dataset::from_ref(ctx, &result.dataset_handle.as_local_ref()).await?;
                CreateDatasetFromSnapshotResult::Success(CreateDatasetResultSuccess { dataset })
            }
            Err(domain::CreateDatasetFromSnapshotError::NameCollision(e)) => {
                CreateDatasetFromSnapshotResult::NameCollision(CreateDatasetResultNameCollision {
                    dataset_name: e.name.into(),
                })
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

///////////////////////////////////////////////////////////////////////////////
// CreateDatasetResult
///////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", type = "String"))]
pub enum CreateDatasetResult {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision),
}

#[derive(Interface, Debug, Clone)]
#[graphql(field(name = "message", type = "String"))]
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

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CreateDatasetResultSuccess {
    pub dataset: Dataset,
}

#[ComplexObject]
impl CreateDatasetResultSuccess {
    async fn message(&self) -> String {
        format!("Success")
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct CreateDatasetResultNameCollision {
    pub dataset_name: DatasetName,
}

#[ComplexObject]
impl CreateDatasetResultNameCollision {
    async fn message(&self) -> String {
        format!("Dataset with name '{}' already exists", self.dataset_name)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub struct CreateDatasetResultInvalidSnapshot {
    pub message: String,
}

///////////////////////////////////////////////////////////////////////////////

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

///////////////////////////////////////////////////////////////////////////////

page_based_connection!(Dataset, DatasetConnection, DatasetEdge);
