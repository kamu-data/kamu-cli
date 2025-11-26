// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_datasets::{DatasetRegistry, DatasetRegistryExt, ExtraDataFields, ResolvedDataset};

use crate::prelude::*;
use crate::queries::molecule::v2::{
    EncryptionMetadata,
    MoleculeAccessLevel,
    MoleculeCategory,
    MoleculeChangeBy,
    MoleculeProjectV2,
    MoleculeTag,
};
use crate::queries::{
    CollectionEntry,
    CollectionProjection,
    Dataset,
    DatasetRequestState,
    FileVersion,
    VersionedFile,
    VersionedFileContentDownload,
    VersionedFileEntry,
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

    async fn latest(&self, ctx: &Context<'_>) -> Result<MoleculeDataRoomProjection<'_>> {
        let projection = self.dataset.as_collection_unchecked().latest(ctx).await?;

        Ok(MoleculeDataRoomProjection {
            projection,
            project: &self.project,
        })
    }

    async fn as_of(
        &self,
        ctx: &Context<'_>,
        block_hash: Multihash<'static>,
    ) -> Result<MoleculeDataRoomProjection<'_>> {
        let projection = self
            .dataset
            .as_collection_unchecked()
            .as_of(ctx, block_hash)
            .await?;

        Ok(MoleculeDataRoomProjection {
            projection,
            project: &self.project,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomProjection<'a> {
    projection: CollectionProjection<'a>,
    project: &'a Arc<MoleculeProjectV2>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomProjection<'_> {
    const DEFAULT_ENTRIES_PER_PAGE: usize = 100;

    async fn entries(
        &self,
        ctx: &Context<'_>,
        path_prefix: Option<CollectionPath>,
        max_depth: Option<usize>,
        page: Option<usize>,
        per_page: Option<usize>,
        filters: Option<MoleculeDataRoomEntriesFilters>,
    ) -> Result<MoleculeDataRoomEntryConnection> {
        assert!(filters.is_none());

        let per_page = per_page.unwrap_or(Self::DEFAULT_ENTRIES_PER_PAGE);

        let entries = self
            .projection
            .entries(ctx, path_prefix, max_depth, page, Some(per_page))
            .await?;

        let molecule_entries = entries
            .nodes
            .into_iter()
            .map(|e| MoleculeDataRoomEntry::new_from_collection_entry(self.project, e))
            .collect::<Result<Vec<_>>>()?;

        // TODO: implement
        Ok(MoleculeDataRoomEntryConnection::new(
            molecule_entries,
            entries.page_info.current_page,
            per_page,
            entries.total_count,
        ))
    }

    async fn entry(
        &self,
        ctx: &Context<'_>,
        path: CollectionPath,
    ) -> Result<Option<MoleculeDataRoomEntry>> {
        let Some(entry) = self.projection.entry(ctx, path).await? else {
            return Ok(None);
        };

        Ok(Some(MoleculeDataRoomEntry::new_from_collection_entry(
            self.project,
            entry,
        )?))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeDataRoomEntriesFilters {
    by_tags: Option<Vec<MoleculeTag>>,
    by_categories: Option<Vec<MoleculeCategory>>,
    by_access_levels: Option<Vec<MoleculeAccessLevel>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeDataRoomEntry
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntry {
    pub entry: CollectionEntry,
    pub project: Arc<MoleculeProjectV2>,
    pub denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
}

impl MoleculeDataRoomEntry {
    pub fn new_from_collection_entry(
        project: &Arc<MoleculeProjectV2>,
        mut entry: CollectionEntry,
    ) -> Result<Self> {
        let mut extra_data = serde_json::Map::default();
        std::mem::swap(&mut entry.entity.extra_data, &mut extra_data);

        let denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom =
            serde_json::from_value(extra_data.into()).int_err()?;

        Ok(Self {
            entry,
            project: project.clone(),
            denormalized_latest_file_info,
        })
    }

    pub fn to_collection_extra_data(&self) -> ExtraData {
        let serde_json::Value::Object(json_map) =
            serde_json::to_value(&self.denormalized_latest_file_info).unwrap()
        else {
            unreachable!()
        };

        ExtraData::new(json_map)
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeDataRoomEntry {
    /// Back link to the project
    async fn project(&self) -> &MoleculeProjectV2 {
        self.project.as_ref()
    }

    /// Access the linked core Dataset
    async fn as_dataset(&self, ctx: &Context<'_>) -> Result<Option<Dataset>> {
        self.entry.as_dataset(ctx).await
    }

    async fn system_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.system_time
    }

    async fn event_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.event_time
    }

    async fn path(&self) -> CollectionPath {
        CollectionPath::from(self.entry.entity.path.clone())
    }

    #[graphql(name = "ref")]
    async fn reference(&self) -> DatasetID<'static> {
        self.entry.entity.reference.clone().into()
    }

    async fn change_by(&self) -> &MoleculeChangeBy {
        &self.denormalized_latest_file_info.change_by
    }

    #[expect(clippy::unused_async)]
    async fn as_versioned_file(&self) -> Result<Option<MoleculeVersionedFile>> {
        Ok(Some(MoleculeVersionedFile {
            dataset: self.entry.entity.reference.clone(),
            prefetched_latest: MoleculeVersionedFilePrefetch::new_from_data_room_entry(self),
        }))
    }
}

page_based_connection!(
    MoleculeDataRoomEntry,
    MoleculeDataRoomEntryConnection,
    MoleculeDataRoomEntryEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeVersionedFile
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFile {
    pub dataset: odf::DatasetID,

    // Filled from denormalized data stored alongside data room entry
    pub prefetched_latest: MoleculeVersionedFilePrefetch,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFile {
    async fn latest(&self, ctx: &Context<'_>) -> Result<Option<MoleculeVersionedFileEntry>> {
        // TODO: PERF: Resolving datasets one by one is inefficient
        let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);
        let dataset = dataset_registry
            .get_dataset_by_ref(&self.dataset.as_local_ref())
            .await
            .int_err()?;

        Ok(Some(MoleculeVersionedFileEntry::new_from_prefetched(
            dataset,
            self.prefetched_latest.clone(),
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileEntry {
    pub entry: VersionedFileEntry,
    pub basic_info: MoleculeVersionedFileEntryBasicInfo,
    pub detailed_info: tokio::sync::OnceCell<MoleculeVersionedFileEntryDetailedInfo>,
}

impl MoleculeVersionedFileEntry {
    pub fn new_from_prefetched(
        dataset: ResolvedDataset,
        prefetch: MoleculeVersionedFilePrefetch,
    ) -> Self {
        Self {
            entry: VersionedFileEntry {
                dataset,
                system_time: prefetch.system_time,
                event_time: prefetch.event_time,
                version: prefetch.denorm.version,
                content_type: prefetch.denorm.content_type,
                content_length: prefetch.denorm.content_length,
                content_hash: prefetch.denorm.content_hash.into(),
                extra_data: ExtraData::default(),
            },
            basic_info: MoleculeVersionedFileEntryBasicInfo {
                access_level: prefetch.denorm.access_level,
                change_by: prefetch.denorm.change_by,
                description: prefetch.denorm.description,
                categories: prefetch.denorm.categories,
                tags: prefetch.denorm.tags,
            },
            detailed_info: tokio::sync::OnceCell::new(),
        }
    }

    pub async fn detailed_info(
        &self,
        ctx: &Context<'_>,
    ) -> Result<&MoleculeVersionedFileEntryDetailedInfo> {
        self.detailed_info
            .get_or_try_init(|| self.read_detailed_info(ctx))
            .await
    }

    pub(crate) async fn read_detailed_info(
        &self,
        ctx: &Context<'_>,
    ) -> Result<MoleculeVersionedFileEntryDetailedInfo> {
        let state = DatasetRequestState::new_resolved(self.entry.dataset.clone());
        let versioned_file = VersionedFile::new(&state);
        let entry = versioned_file
            .get_entry(ctx, Some(self.entry.version), None)
            .await?
            .expect("Entry must exist");
        let json = entry.extra_data.into_inner();
        let detailed_info: MoleculeVersionedFileEntryDetailedInfo =
            serde_json::from_value(json.into()).int_err()?;
        Ok(detailed_info)
    }

    pub fn to_versioned_file_extra_data(&self) -> ExtraDataFields {
        let extra_data = MoleculeVersionedFileExtraData {
            basic_info: &self.basic_info,
            detailed_info: self.detailed_info.get().unwrap(),
        };

        let serde_json::Value::Object(json) = serde_json::to_value(&extra_data).unwrap() else {
            unreachable!()
        };

        ExtraDataFields::new(json)
    }

    pub fn to_denormalized(&self) -> MoleculeDenormalizeFileToDataRoom {
        MoleculeDenormalizeFileToDataRoom {
            access_level: self.basic_info.access_level.clone(),
            change_by: self.basic_info.change_by.clone(),
            version: self.entry.version,
            content_type: self.entry.content_type.clone(),
            content_length: self.entry.content_length,
            content_hash: self.entry.content_hash.clone().into(),
            description: self.basic_info.description.clone(),
            categories: self.basic_info.categories.clone(),
            tags: self.basic_info.tags.clone(),
            // TODO
            // content_text
            // encryption_metadata
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileEntry {
    async fn system_time(&self) -> &DateTime<Utc> {
        &self.entry.system_time
    }

    async fn event_time(&self) -> &DateTime<Utc> {
        &self.entry.event_time
    }

    async fn version(&self) -> u32 {
        self.entry.version
    }

    async fn content_hash(&self) -> &Multihash<'static> {
        &self.entry.content_hash
    }

    async fn content_length(&self) -> usize {
        self.entry.content_length
    }

    async fn content_type(&self) -> &String {
        &self.entry.content_type
    }

    async fn access_level(&self) -> &MoleculeAccessLevel {
        &self.basic_info.access_level
    }

    async fn change_by(&self) -> &String {
        &self.basic_info.change_by
    }

    async fn description(&self) -> &Option<String> {
        &self.basic_info.description
    }

    async fn categories(&self) -> &Vec<MoleculeCategory> {
        &self.basic_info.categories
    }

    async fn tags(&self) -> &Vec<MoleculeTag> {
        &self.basic_info.tags
    }

    async fn content_text(&self, ctx: &Context<'_>) -> Result<&Option<String>> {
        Ok(&self.detailed_info(ctx).await?.content_text)
    }

    async fn encryption_metadata(&self, ctx: &Context<'_>) -> Result<&Option<EncryptionMetadata>> {
        Ok(&self.detailed_info(ctx).await?.encryption_metadata)
    }

    /// Returns encoded content in-band. Should be used for small files only and
    /// will return an error if called on large data.
    pub async fn content(&self, ctx: &Context<'_>) -> Result<Base64Usnp> {
        self.entry.content(ctx).await
    }

    /// Returns a direct download URL
    async fn content_url(&self, ctx: &Context<'_>) -> Result<VersionedFileContentDownload> {
        self.entry.content_url(ctx).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeVersionedFileEntryBasicInfo {
    #[serde(rename = "molecule_access_level")]
    pub access_level: MoleculeAccessLevel,
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,
    pub description: Option<String>,
    pub categories: Vec<MoleculeCategory>,
    pub tags: Vec<MoleculeTag>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeVersionedFileEntryDetailedInfo {
    pub content_text: Option<String>,
    pub encryption_metadata: Option<EncryptionMetadata>,
}

#[derive(serde::Serialize)]
pub struct MoleculeVersionedFileExtraData<'a> {
    #[serde(flatten)]
    basic_info: &'a MoleculeVersionedFileEntryBasicInfo,

    #[serde(flatten)]
    detailed_info: &'a MoleculeVersionedFileEntryDetailedInfo,
}

#[derive(Clone)]
pub struct MoleculeVersionedFilePrefetch {
    pub system_time: DateTime<Utc>,
    pub event_time: DateTime<Utc>,
    pub denorm: MoleculeDenormalizeFileToDataRoom,
}

impl MoleculeVersionedFilePrefetch {
    pub fn new_from_data_room_entry(data_room_entry: &MoleculeDataRoomEntry) -> Self {
        Self {
            system_time: data_room_entry.entry.entity.system_time,
            event_time: data_room_entry.entry.entity.event_time,
            denorm: data_room_entry.denormalized_latest_file_info.clone(),
        }
    }
}

/// These fields are stored as extra columns in data room collection
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDenormalizeFileToDataRoom {
    pub version: FileVersion,
    pub content_type: String,
    pub content_length: usize,
    pub content_hash: odf::Multihash,

    #[serde(rename = "molecule_access_level")]
    pub access_level: MoleculeAccessLevel,
    #[serde(rename = "molecule_change_by")]
    pub change_by: MoleculeChangeBy,
    pub description: Option<String>,
    pub categories: Vec<MoleculeCategory>,
    pub tags: Vec<MoleculeTag>,
    // TODO:
    // pub content_text: String,
    // pub encryption_metadata: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
