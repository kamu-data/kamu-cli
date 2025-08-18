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
use crate::{LoggedInGuard, utils};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetsMut;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl DatasetsMut {
    fn parse_metadata_events(
        &self,
        events: Vec<String>,
        events_format: Option<MetadataManifestFormat>,
    ) -> Result<
        Vec<odf::metadata::MetadataEvent>,
        Result<Box<CreateDatasetFromSnapshotResult>, GqlError>,
    > {
        let mut res = Vec::new();
        match events_format.unwrap_or(MetadataManifestFormat::Yaml) {
            MetadataManifestFormat::Yaml => {
                let de = odf::metadata::serde::yaml::YamlMetadataEventDeserializer;
                for event in events {
                    match de.read_manifest(event.as_bytes()) {
                        Ok(event) => res.push(event),
                        Err(e @ odf::metadata::serde::Error::SerdeError { .. }) => {
                            return Err(Ok(Box::new(CreateDatasetFromSnapshotResult::Malformed(
                                MetadataManifestMalformed {
                                    message: e.to_string(),
                                },
                            ))));
                        }
                        Err(odf::metadata::serde::Error::UnsupportedVersion(e)) => {
                            return Err(Ok(Box::new(
                                CreateDatasetFromSnapshotResult::UnsupportedVersion(e.into()),
                            )));
                        }
                        Err(e @ odf::metadata::serde::Error::IoError { .. }) => {
                            return Err(Err(e.int_err().into()));
                        }
                    }
                }
            }
        }
        Ok(res)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl DatasetsMut {
    /// Returns a mutable dataset by its ID, if found
    #[tracing::instrument(level = "info", name = DatasetsMut_by_id, skip_all, fields(%dataset_id))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn by_id(
        &self,
        ctx: &Context<'_>,
        dataset_id: DatasetID<'_>,
    ) -> Result<Option<DatasetMut>> {
        use kamu_core::DatasetRegistryExt;

        let dataset_registry = from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);

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

    /// Returns mutable datasets by their IDs
    #[tracing::instrument(level = "info", name = DatasetsMut_by_ids, skip_all, fields(?dataset_ids))]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn by_ids(
        &self,
        ctx: &Context<'_>,
        dataset_ids: Vec<DatasetID<'_>>,
        #[graphql(
            desc = "Whether to skip unresolved datasets or return an error if one or more are \
                    missing"
        )]
        skip_missing: bool,
    ) -> Result<Vec<DatasetMut>> {
        let dataset_ids: Vec<std::borrow::Cow<odf::DatasetID>> = dataset_ids
            .into_iter()
            .map(|id| std::borrow::Cow::Owned(id.into()))
            .collect();

        let dataset_registry = from_catalog_n!(ctx, dyn kamu_core::DatasetRegistry);

        let resolution = dataset_registry
            .resolve_multiple_dataset_handles_by_ids(&dataset_ids)
            .await
            .int_err()?;

        if !skip_missing && !resolution.unresolved_datasets.is_empty() {
            return Err(GqlError::gql(format!(
                "Unresolved dataset: {}",
                resolution.unresolved_datasets.into_iter().next().unwrap().0
            )));
        }

        let mut res = Vec::new();

        // TODO: PERF: Vectorize access checks
        for hdl in resolution.resolved_handles {
            let dataset_request_state = DatasetRequestState::new(hdl);
            let allowed_actions = dataset_request_state.allowed_dataset_actions(ctx).await?;
            let current_dataset_action = auth::DatasetAction::Write;

            let dataset = match auth::DatasetAction::resolve_access(
                allowed_actions,
                current_dataset_action,
            ) {
                DatasetActionAccess::Full => DatasetMut::new_access_checked(dataset_request_state),
                DatasetActionAccess::Limited => {
                    return Err(utils::make_dataset_access_error(
                        dataset_request_state.dataset_handle(),
                    ));
                }
                DatasetActionAccess::Forbidden => {
                    if skip_missing {
                        continue;
                    }
                    return Err(GqlError::gql(format!(
                        "Unresolved dataset: {}",
                        dataset_request_state.dataset_handle().id
                    )));
                }
            };

            res.push(dataset);
        }

        Ok(res)
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
                        ));
                    }
                    Err(e @ odf::metadata::serde::Error::IoError { .. }) => {
                        return Err(e.int_err().into());
                    }
                }
            }
        };

        self.create_from_snapshot_impl(ctx, snapshot, dataset_visibility.into())
            .await
    }

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
            Err(CreateDatasetFromSnapshotError::Access(e)) => {
                return Err(e.into());
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
            Err(CreateDatasetFromSnapshotError::InvalidBlock(e)) => {
                CreateDatasetFromSnapshotResult::InvalidSnapshot(
                    CreateDatasetResultInvalidSnapshot { message: e.message },
                )
            }
            Err(CreateDatasetFromSnapshotError::CASFailed(e)) => return Err(e.int_err().into()),
            Err(CreateDatasetFromSnapshotError::Internal(e)) => return Err(e.into()),
        };

        Ok(result)
    }

    /// Creates new versioned file dataset.
    /// Can include schema for extra columns and dataset metadata events (e.g.
    /// adding description and readme).
    #[tracing::instrument(level = "info", name = DatasetsMut_create_versioned_file, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_versioned_file(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Dataset alias (may include target account)")] dataset_alias: DatasetAlias<
            '_,
        >,
        #[graphql(desc = "Additional user-defined columns")] extra_columns: Option<
            Vec<ColumnInput>,
        >,
        #[graphql(desc = "Extra metadata events (e.g. to populate readme)")] extra_events: Option<
            Vec<String>,
        >,
        #[graphql(desc = "How extra events are represented")] extra_events_format: Option<
            MetadataManifestFormat,
        >,
        #[graphql(desc = "Visibility of the dataset")] dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        let extra_events_parsed = match self
            .parse_metadata_events(extra_events.unwrap_or_default(), extra_events_format)
        {
            Ok(res) => res,
            Err(Ok(err)) => return Ok(*err),
            Err(Err(err)) => return Err(err),
        };

        let snapshot = match crate::queries::VersionedFile::dataset_snapshot(
            dataset_alias.into(),
            extra_columns.unwrap_or_default(),
            extra_events_parsed,
        ) {
            Ok(s) => s,
            Err(e) => {
                return Ok(CreateDatasetFromSnapshotResult::InvalidSnapshot(
                    CreateDatasetResultInvalidSnapshot {
                        message: e.to_string(),
                    },
                ));
            }
        };

        self.create_from_snapshot_impl(ctx, snapshot, dataset_visibility.into())
            .await
    }

    /// Creates a new collection dataset.
    /// Can include schema for extra columns, dataset metadata, and initial
    /// collection entries.
    #[tracing::instrument(level = "info", name = DatasetsMut_create_collection, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_collection(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Dataset alias (may include target account)")] dataset_alias: DatasetAlias<
            '_,
        >,
        #[graphql(desc = "Additional user-defined columns")] extra_columns: Option<
            Vec<ColumnInput>,
        >,
        #[graphql(desc = "Extra metadata events (e.g. to populate readme)")] extra_events: Option<
            Vec<String>,
        >,
        #[graphql(desc = "How extra events are represented")] extra_events_format: Option<
            MetadataManifestFormat,
        >,
        #[graphql(desc = "Visibility of the dataset")] dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult> {
        let extra_events_parsed = match self
            .parse_metadata_events(extra_events.unwrap_or_default(), extra_events_format)
        {
            Ok(res) => res,
            Err(Ok(err)) => return Ok(*err),
            Err(Err(err)) => return Err(err),
        };

        let snapshot = match crate::queries::Collection::dataset_snapshot(
            dataset_alias.into(),
            extra_columns.unwrap_or_default(),
            extra_events_parsed,
        ) {
            Ok(s) => s,
            Err(e) => {
                return Ok(CreateDatasetFromSnapshotResult::InvalidSnapshot(
                    CreateDatasetResultInvalidSnapshot {
                        message: e.to_string(),
                    },
                ));
            }
        };

        self.create_from_snapshot_impl(ctx, snapshot, dataset_visibility.into())
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// CreateDatasetResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
pub enum CreateDatasetResult<'a> {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision<'a>),
}

#[derive(Interface, Debug)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "message", ty = "String")
)]
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
    pub async fn is_success(&self) -> bool {
        true
    }
    pub async fn message(&self) -> String {
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
    pub async fn is_success(&self) -> bool {
        false
    }
    pub async fn message(&self) -> String {
        format!("Dataset with name '{}' already exists", self.dataset_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultInvalidSnapshot {
    pub message: String,
}

#[ComplexObject]
impl CreateDatasetResultInvalidSnapshot {
    pub async fn is_success(&self) -> bool {
        false
    }
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
    pub async fn is_success(&self) -> bool {
        false
    }
    pub async fn message(&self) -> String {
        format!(
            "Dataset is referencing non-existing inputs: {}",
            self.missing_inputs.join(", ")
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
