// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use file_utils::MediaType;
use kamu_datasets::FileVersion;
use kamu_molecule_domain::{
    MoleculeDataRoomActivity,
    MoleculeFindDataRoomEntryError,
    MoleculeFindDataRoomEntryUseCase,
    MoleculeViewDataRoomEntriesError,
    MoleculeViewDataRoomEntriesUseCase,
};

use crate::prelude::*;
use crate::queries::Dataset;
use crate::queries::molecule::v2::{
    MoleculeAccessLevel,
    MoleculeCategory,
    MoleculeChangeBy,
    MoleculeProjectV2,
    MoleculeTag,
    MoleculeVersionedFile,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeDataRoom
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoom {
    pub dataset: Dataset,
    pub project: Arc<MoleculeProjectV2>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoom {
    /// Access the underlying core Dataset
    async fn dataset(&self) -> &Dataset {
        &self.dataset
    }

    #[expect(clippy::unused_async)]
    async fn latest(&self) -> Result<MoleculeDataRoomProjection<'_>> {
        Ok(MoleculeDataRoomProjection {
            project: &self.project,
            as_of: None,
        })
    }

    #[expect(clippy::unused_async)]
    async fn as_of(
        &self,
        block_hash: Multihash<'static>,
    ) -> Result<MoleculeDataRoomProjection<'_>> {
        Ok(MoleculeDataRoomProjection {
            project: &self.project,
            as_of: Some(block_hash.into()),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomProjection<'a> {
    project: &'a Arc<MoleculeProjectV2>,
    as_of: Option<odf::Multihash>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomProjection<'_> {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    async fn entries(
        &self,
        ctx: &Context<'_>,
        path_prefix: Option<CollectionPath<'static>>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
    ) -> Result<MoleculeDataRoomEntryConnection> {
        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);
        let page = page.unwrap_or(0);

        let view_data_room_entries_uc =
            from_catalog_n!(ctx, dyn MoleculeViewDataRoomEntriesUseCase);

        let molecule_entries_listing = view_data_room_entries_uc
            .execute(
                &self.project.entity,
                self.as_of.clone(),
                path_prefix.map(Into::into),
                max_depth,
                filters.map(Into::into),
                Some(PaginationOpts {
                    limit: per_page,
                    offset: page * per_page,
                }),
            )
            .await
            .map_err(|e| -> GqlError {
                use MoleculeViewDataRoomEntriesError as E;
                match e {
                    E::Access(e) => e.into(),
                    E::Internal(_) => e.int_err().into(),
                }
            })?;

        let api_entry_nodes = molecule_entries_listing
            .list
            .into_iter()
            .map(|e| {
                MoleculeDataRoomEntry::new_from_data_room_entry(
                    self.project,
                    e,
                    self.as_of.is_none(),
                )
            })
            .collect::<Vec<_>>();

        Ok(MoleculeDataRoomEntryConnection::new(
            api_entry_nodes,
            page,
            per_page,
            molecule_entries_listing.total_count,
        ))
    }

    async fn entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath<'static>,
    ) -> Result<Option<MoleculeDataRoomEntry>> {
        let find_data_room_entry_uc = from_catalog_n!(ctx, dyn MoleculeFindDataRoomEntryUseCase);

        let maybe_entry = find_data_room_entry_uc
            .execute_find_by_path(&self.project.entity, self.as_of.clone(), path.into())
            .await
            .map_err(|e| match e {
                MoleculeFindDataRoomEntryError::Access(e) => GqlError::Access(e),
                MoleculeFindDataRoomEntryError::Internal(e) => e.int_err().into(),
            })?;

        let maybe_api_entry = maybe_entry.map(|e| {
            MoleculeDataRoomEntry::new_from_data_room_entry(self.project, e, self.as_of.is_none())
        });

        Ok(maybe_api_entry)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeDataRoomEntriesFilters {
    by_tags: Option<Vec<MoleculeTag>>,
    by_categories: Option<Vec<MoleculeCategory>>,
    by_access_levels: Option<Vec<MoleculeAccessLevel>>,
}

impl From<MoleculeDataRoomEntriesFilters> for kamu_molecule_domain::MoleculeDataRoomEntriesFilters {
    fn from(value: MoleculeDataRoomEntriesFilters) -> Self {
        Self {
            by_tags: value.by_tags,
            by_categories: value.by_categories,
            by_access_levels: value.by_access_levels,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeDataRoomEntry
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntry {
    pub entity: kamu_molecule_domain::MoleculeDataRoomEntry,
    project: Arc<MoleculeProjectV2>,
    is_latest_data_room_entry: bool,
}

impl MoleculeDataRoomEntry {
    pub fn new_from_data_room_entry(
        project: &Arc<MoleculeProjectV2>,
        data_room_entry: kamu_molecule_domain::MoleculeDataRoomEntry,
        is_latest_data_room_entry: bool,
    ) -> Self {
        Self {
            entity: data_room_entry,
            project: project.clone(),
            is_latest_data_room_entry,
        }
    }

    pub fn new_from_data_room_activity_entity(
        project: &Arc<MoleculeProjectV2>,
        activity_entity: MoleculeDataRoomActivity,
    ) -> Self {
        let entity = kamu_molecule_domain::MoleculeDataRoomEntry {
            system_time: activity_entity.system_time,
            event_time: activity_entity.event_time,
            path: activity_entity.path,
            reference: activity_entity.r#ref,
            denormalized_latest_file_info:
                kamu_molecule_domain::MoleculeDenormalizeFileToDataRoom {
                    version: activity_entity.version,
                    content_type: activity_entity
                        .content_type
                        .unwrap_or_else(|| MediaType::OCTET_STREAM.to_owned()),
                    content_length: activity_entity.content_length,
                    content_hash: activity_entity.content_hash,
                    access_level: activity_entity.access_level,
                    change_by: activity_entity.change_by,
                    description: activity_entity.description,
                    categories: activity_entity.categories,
                    tags: activity_entity.tags,
                },
        };

        Self {
            entity,
            project: project.clone(),
            is_latest_data_room_entry: false,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntry {
    /// Backlink to the project
    async fn project(&self) -> &MoleculeProjectV2 {
        self.project.as_ref()
    }

    /// Access the linked core Dataset
    async fn as_dataset(&self, ctx: &Context<'_>) -> Result<Option<Dataset>> {
        Dataset::try_from_ref(ctx, &self.entity.reference.as_local_ref()).await
    }

    async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    async fn path(&self) -> CollectionPath<'_> {
        CollectionPath::from(&self.entity.path)
    }

    #[graphql(name = "ref")]
    async fn reference(&self) -> DatasetID<'_> {
        DatasetID::from(&self.entity.reference)
    }

    // TODO: Do we need these fields here? -->
    async fn change_by(&self) -> &MoleculeChangeBy {
        &self.entity.denormalized_latest_file_info.change_by
    }

    async fn access_level(&self) -> &MoleculeAccessLevel {
        &self.entity.denormalized_latest_file_info.access_level
    }
    // <--

    #[expect(clippy::unused_async)]
    async fn as_versioned_file(&self) -> Result<MoleculeVersionedFile<'_>> {
        Ok(MoleculeVersionedFile::new(
            Cow::Borrowed(&self.entity),
            self.is_latest_data_room_entry,
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    MoleculeDataRoomEntry,
    MoleculeDataRoomEntryConnection,
    MoleculeDataRoomEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// These fields are stored as extra columns in data room collection
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDenormalizeFileToDataRoom {
    pub version: FileVersion,
    pub content_type: MediaType,
    pub content_length: usize,
    pub content_hash: odf::Multihash,

    #[serde(rename = "molecule_access_level")]
    pub access_level: MoleculeAccessLevel,
    #[serde(rename = "molecule_change_by")]
    pub change_by: MoleculeChangeBy,
    pub description: Option<String>,
    pub categories: Vec<MoleculeCategory>,
    pub tags: Vec<MoleculeTag>,
}

impl From<kamu_molecule_domain::MoleculeDenormalizeFileToDataRoom>
    for MoleculeDenormalizeFileToDataRoom
{
    fn from(denorm: kamu_molecule_domain::MoleculeDenormalizeFileToDataRoom) -> Self {
        Self {
            version: denorm.version,
            content_type: denorm.content_type,
            content_length: denorm.content_length,
            content_hash: denorm.content_hash,
            access_level: denorm.access_level,
            change_by: denorm.change_by,
            description: denorm.description,
            categories: denorm.categories,
            tags: denorm.tags,
        }
    }
}

impl From<MoleculeDenormalizeFileToDataRoom>
    for kamu_molecule_domain::MoleculeDenormalizeFileToDataRoom
{
    fn from(denorm: MoleculeDenormalizeFileToDataRoom) -> Self {
        Self {
            version: denorm.version,
            content_type: denorm.content_type,
            content_length: denorm.content_length,
            content_hash: denorm.content_hash,
            access_level: denorm.access_level,
            change_by: denorm.change_by,
            description: denorm.description,
            categories: denorm.categories,
            tags: denorm.tags,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
