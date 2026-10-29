// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::{CreateDatasetFromSnapshotError, DatasetPermission, DatasetPermissionAccess};

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
        Result<Box<CreateDatasetFromSnapshotResult<'_>>, GqlError>,
    > {
        let mut res = Vec::new();
        match events_format.unwrap_or(MetadataManifestFormat::Yaml) {
            MetadataManifestFormat::Yaml => {
                let de = odf::metadata::serde::yaml::YamlMetadataEventDeserializer;
                for event in events {
                    match de.read_manifest(event.as_bytes()) {
                        Ok(event) => res.push(event),
                        Err(
                            e @ (odf::metadata::serde::Error::SerdeError { .. }
                            | odf::metadata::serde::Error::Validation(_)),
                        ) => {
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

/// Mutations that look up and create datasets
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
        // TODO: PERF: GQL: DataLoader?

        use kamu_datasets::DatasetRegistryExt;

        let dataset_registry = from_catalog_n!(ctx, dyn kamu_datasets::DatasetRegistry);

        let maybe_dataset_handle = dataset_registry
            .try_resolve_dataset_handle_by_ref(&dataset_id.as_local_ref())
            .await?;
        let Some(dataset_handle) = maybe_dataset_handle else {
            return Ok(None);
        };

        let dataset_request_state = DatasetRequestState::new(dataset_handle);
        let allowed_actions = dataset_request_state
            .granted_dataset_permissions(ctx)
            .await?;
        let current_dataset_permission = DatasetPermission::Write;

        match DatasetPermission::resolve_access(allowed_actions, current_dataset_permission) {
            DatasetPermissionAccess::Allowed => {
                Ok(Some(DatasetMut::new_access_checked(dataset_request_state)))
            }
            DatasetPermissionAccess::Insufficient => Err(utils::make_dataset_access_error(
                dataset_request_state.dataset_handle(),
            )),
            DatasetPermissionAccess::Forbidden => Ok(None),
        }
    }

    /// Returns mutable datasets by their IDs.
    ///
    /// Order of results is guaranteed to match the inputs. Duplicate inputs
    /// will results in duplicate results.
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
        // TODO: PERF: GQL: DataLoader?

        let rebac_dataset_registry_facade =
            from_catalog_n!(ctx, dyn kamu_auth_rebac::RebacDatasetRegistryFacade);

        let dataset_refs = dataset_ids
            .into_iter()
            .map(|id| id.as_ref().as_local_ref())
            .collect::<Vec<_>>();
        let dataset_refs_refs = dataset_refs.iter().collect::<Vec<_>>();

        let resolution = rebac_dataset_registry_facade
            .classify_dataset_refs_by_access(&dataset_refs_refs, DatasetPermission::Write)
            .await?;

        if let Some(error_msg) = resolution.try_get_user_error_report(skip_missing) {
            return Err(GqlError::gql(error_msg));
        }

        // Reorder resolved handles to match the order of inputs
        let accessible_dataset_handles: Vec<odf::DatasetHandle> = {
            let resolved_handles_map: std::collections::BTreeMap<
                odf::DatasetRef,
                odf::DatasetHandle,
            > = resolution.allowed.into_iter().collect();

            dataset_refs
                .iter()
                .filter_map(|r| resolved_handles_map.get(r).cloned())
                .collect()
        };

        let dataset_muts = accessible_dataset_handles
            .into_iter()
            .map(|dataset_handle| {
                let dataset_request_state = DatasetRequestState::new(dataset_handle);
                DatasetMut::new_access_checked(dataset_request_state)
            })
            .collect();

        Ok(dataset_muts)
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
    ) -> Result<CreateDatasetResult<'_>> {
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

    /// Creates a new dataset from provided snapshot manifest
    #[tracing::instrument(level = "info", name = DatasetsMut_create_from_snapshot, skip_all)]
    #[graphql(guard = "LoggedInGuard::new()")]
    async fn create_from_snapshot(
        &self,
        ctx: &Context<'_>,
        snapshot: String,
        snapshot_format: MetadataManifestFormat,
        dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult<'_>> {
        use odf::metadata::serde::DatasetSnapshotDeserializer;

        let snapshot = match snapshot_format {
            MetadataManifestFormat::Yaml => {
                let de = odf::metadata::serde::yaml::YamlDatasetSnapshotDeserializer;
                match de.read_manifest(snapshot.as_bytes()) {
                    Ok(snapshot) => snapshot,
                    Err(
                        e @ (odf::metadata::serde::Error::SerdeError { .. }
                        | odf::metadata::serde::Error::Validation(_)),
                    ) => {
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
    ) -> Result<CreateDatasetFromSnapshotResult<'_>> {
        let create_from_snapshot =
            from_catalog_n!(ctx, dyn kamu_datasets::CreateDatasetFromSnapshotUseCase);

        let create_options = kamu_datasets::CreateDatasetUseCaseOptions {
            dataset_visibility,
            ..Default::default()
        };

        let result = match create_from_snapshot.execute(snapshot, create_options).await {
            Ok(result) => {
                let account = Account::from_dataset_alias(ctx, &result.dataset_handle.alias)
                    .await?
                    .expect("Account must exist");
                let dataset = Dataset::from_resolved_authorized_dataset(
                    account,
                    &kamu_datasets::ResolvedDataset::from_created(&result),
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
        #[graphql(desc = "DEPRECATED: Additional user-defined columns")] extra_columns: Option<
            Vec<ColumnInput>,
        >,
        #[graphql(desc = "User-defined schema that will be merged with base")] extra_schema: Option<
            DataSchemaInput,
        >,
        #[graphql(desc = "Extra metadata events (e.g. to populate readme)")] extra_events: Option<
            Vec<String>,
        >,
        #[graphql(desc = "How extra events are represented")] extra_events_format: Option<
            MetadataManifestFormat,
        >,
        #[graphql(desc = "Visibility of the dataset")] dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult<'_>> {
        let extra_events_parsed = match self
            .parse_metadata_events(extra_events.unwrap_or_default(), extra_events_format)
        {
            Ok(res) => res,
            Err(Ok(err)) => return Ok(*err),
            Err(Err(err)) => return Err(err),
        };

        let snapshot = match crate::queries::VersionedFile::dataset_snapshot(
            dataset_alias.into(),
            extra_schema,
            extra_columns,
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
        #[graphql(desc = "DEPRECATED Additional user-defined columns")] extra_columns: Option<
            Vec<ColumnInput>,
        >,
        #[graphql(desc = "User-defined schema that will be merged with base")] extra_schema: Option<
            DataSchemaInput,
        >,
        #[graphql(desc = "Extra metadata events (e.g. to populate readme)")] extra_events: Option<
            Vec<String>,
        >,
        #[graphql(desc = "How extra events are represented")] extra_events_format: Option<
            MetadataManifestFormat,
        >,
        #[graphql(desc = "Visibility of the dataset")] dataset_visibility: DatasetVisibility,
    ) -> Result<CreateDatasetFromSnapshotResult<'_>> {
        let extra_events_parsed = match self
            .parse_metadata_events(extra_events.unwrap_or_default(), extra_events_format)
        {
            Ok(res) => res,
            Err(Ok(err)) => return Ok(*err),
            Err(Err(err)) => return Err(err),
        };

        let snapshot = match crate::queries::Collection::dataset_snapshot(
            dataset_alias.into(),
            extra_schema,
            extra_columns,
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

/// Outcome of creating an empty dataset
#[derive(Interface, Debug)]
#[graphql(
    field(
        name = "is_success",
        ty = "bool",
        desc = "Whether the operation succeeded"
    ),
    field(
        name = "message",
        ty = "String",
        desc = "Human-readable outcome message"
    )
)]
pub enum CreateDatasetResult<'a> {
    Success(CreateDatasetResultSuccess),
    NameCollision(CreateDatasetResultNameCollision<'a>),
}

/// Outcome of creating a dataset from a snapshot
#[derive(Interface, Debug)]
#[graphql(
    field(
        name = "is_success",
        ty = "bool",
        desc = "Whether the operation succeeded"
    ),
    field(
        name = "message",
        ty = "String",
        desc = "Human-readable outcome message"
    )
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

/// Dataset was created
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultSuccess {
    /// Created dataset
    pub dataset: Dataset,
}

#[ComplexObject]
impl CreateDatasetResultSuccess {
    /// Always true
    pub async fn is_success(&self) -> bool {
        true
    }
    /// Human-readable outcome message
    pub async fn message(&self) -> String {
        "Success".to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Dataset with the requested name already exists
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultNameCollision<'a> {
    /// Owner account of the existing dataset, null in single-tenant mode
    pub account_name: Option<AccountName<'a>>,
    /// Name that is already taken
    pub dataset_name: DatasetName<'a>,
}

#[ComplexObject]
impl CreateDatasetResultNameCollision<'_> {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    pub async fn message(&self) -> String {
        format!("Dataset with name '{}' already exists", self.dataset_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Snapshot or the events in it are invalid
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultInvalidSnapshot {
    /// Reason the snapshot was rejected
    pub message: String,
}

#[ComplexObject]
impl CreateDatasetResultInvalidSnapshot {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Derivative dataset references inputs that do not exist
#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateDatasetResultMissingInputs {
    // TODO: Input can be referenced by ID or by name
    // do we need DatasetRef-style scalar for GQL?
    /// References to the inputs that could not be resolved
    pub missing_inputs: Vec<String>,
}

#[ComplexObject]
impl CreateDatasetResultMissingInputs {
    /// Always false
    pub async fn is_success(&self) -> bool {
        false
    }
    /// Human-readable error message
    pub async fn message(&self) -> String {
        format!(
            "Dataset is referencing non-existing inputs: {}",
            self.missing_inputs.join(", ")
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
