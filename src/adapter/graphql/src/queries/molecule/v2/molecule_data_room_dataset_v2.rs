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
use kamu_datasets::{ExtraDataFields, ResolvedDataset};
use kamu_molecule_domain::MoleculeDataRoomActivityEntity;

use crate::data_loader::AccessCheckedDatasetRef;
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
use crate::utils;

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
        path_prefix: Option<CollectionPath<'static>>,
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
        path: CollectionPath<'static>,
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
    pub fn new_from_json(
        mut value: serde_json::Value,
        project: &Arc<MoleculeProjectV2>,
        vocab: &odf::metadata::DatasetVocabulary,
    ) -> Result<(odf::metadata::OperationType, Self)> {
        let Some(obj) = value.as_object_mut() else {
            unreachable!()
        };
        let Some(raw_op) = obj[&vocab.operation_type_column].as_i64() else {
            unreachable!()
        };

        let op = odf::metadata::OperationType::try_from(u8::try_from(raw_op).unwrap()).unwrap();

        let entity = kamu_datasets::CollectionEntry::from_json(value).int_err()?;
        let collection_entry = CollectionEntry::new(entity);
        let dataroom_entry =
            MoleculeDataRoomEntry::new_from_collection_entry(project, collection_entry.clone())?;

        Ok((op, dataroom_entry))
    }

    pub fn new_from_collection_entry(
        project: &Arc<MoleculeProjectV2>,
        mut entry: CollectionEntry,
    ) -> Result<Self> {
        let mut extra_data = kamu_datasets::ExtraDataFields::default();
        std::mem::swap(&mut entry.entity.extra_data, &mut extra_data);

        let denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom =
            serde_json::from_value(extra_data.into_inner().into()).int_err()?;

        Ok(Self {
            entry,
            project: project.clone(),
            denormalized_latest_file_info,
        })
    }

    pub fn new_from_data_room_activity_entity(
        project: &Arc<MoleculeProjectV2>,
        activity_entity: MoleculeDataRoomActivityEntity,
    ) -> Self {
        let entry = CollectionEntry {
            entity: kamu_datasets::CollectionEntry {
                system_time: activity_entity.system_time,
                event_time: activity_entity.event_time,
                path: activity_entity.path,
                reference: activity_entity.r#ref,
                extra_data: Default::default(),
            },
        };
        let denormalized_latest_file_info = MoleculeDenormalizeFileToDataRoom {
            version: activity_entity.version,
            content_type: activity_entity.content_type.unwrap_or_else(|| "".into()).0,
            content_length: activity_entity.content_length,
            content_hash: activity_entity.content_hash,
            access_level: activity_entity.access_level,
            change_by: activity_entity.change_by,
            description: activity_entity.description,
            categories: activity_entity.categories,
            tags: activity_entity.tags,
        };

        Self {
            entry,
            denormalized_latest_file_info,
            project: project.clone(),
        }
    }

    pub fn to_collection_extra_data(&self) -> ExtraData {
        let serde_json::Value::Object(json_map) =
            serde_json::to_value(&self.denormalized_latest_file_info).unwrap()
        else {
            unreachable!()
        };

        ExtraData::new(json_map)
    }

    pub fn is_same_reference(&self, other: &Self) -> bool {
        self.entry.entity.reference == other.entry.entity.reference
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
        self.entry.as_dataset(ctx).await
    }

    async fn system_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.system_time
    }

    async fn event_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.event_time
    }

    async fn path(&self) -> CollectionPath<'_> {
        CollectionPath::from(&self.entry.entity.path)
    }

    #[graphql(name = "ref")]
    async fn reference(&self) -> DatasetID<'_> {
        DatasetID::from(&self.entry.entity.reference)
    }

    async fn change_by(&self) -> &MoleculeChangeBy {
        &self.denormalized_latest_file_info.change_by
    }

    #[expect(clippy::unused_async)]
    async fn as_versioned_file(&self) -> Result<Option<MoleculeVersionedFile>> {
        Ok(Some(MoleculeVersionedFile {
            dataset_id: self.entry.entity.reference.clone(),
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
    pub dataset_id: odf::DatasetID,

    // Filled from denormalized data stored alongside data room entry
    pub prefetched_latest: MoleculeVersionedFilePrefetch,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFile {
    // TODO: For the list of activities, do we need to display the version at the
    //       time of activity here (specify head)?
    async fn latest(&self, ctx: &Context<'_>) -> Result<Option<MoleculeVersionedFileEntry>> {
        let dataset_handle_data_loader = utils::get_dataset_handle_data_loader(ctx);

        let maybe_resolved_dataset = dataset_handle_data_loader
            .load_one(AccessCheckedDatasetRef::new(self.dataset_id.as_local_ref()))
            .await
            .map_err(data_loader_error_mapper)?;

        if let Some(resolved_dataset) = maybe_resolved_dataset {
            Ok(Some(MoleculeVersionedFileEntry::new_from_prefetched(
                resolved_dataset,
                self.prefetched_latest.clone(),
            )))
        } else {
            Ok(None)
        }
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
        file_dataset: ResolvedDataset,
        prefetch: MoleculeVersionedFilePrefetch,
    ) -> Self {
        Self {
            entry: VersionedFileEntry {
                file_dataset,
                entity: kamu_datasets::VersionedFileEntry {
                    system_time: prefetch.system_time,
                    event_time: prefetch.event_time,
                    version: prefetch.denorm.version,
                    content_type: prefetch.denorm.content_type,
                    content_length: prefetch.denorm.content_length,
                    content_hash: prefetch.denorm.content_hash,
                    extra_data: kamu_datasets::ExtraDataFields::default(),
                },
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
        let state = DatasetRequestState::new_resolved(self.entry.file_dataset.clone());
        let versioned_file = VersionedFile::new(&state);
        let entry = versioned_file
            .get_entry(ctx, Some(self.entry.entity.version), None)
            .await?
            .expect("Entry must exist");
        let json = entry.entity.extra_data.into_inner();
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
            version: self.entry.entity.version,
            content_type: self.entry.entity.content_type.clone(),
            content_length: self.entry.entity.content_length,
            content_hash: self.entry.entity.content_hash.clone(),
            description: self.basic_info.description.clone(),
            categories: self.basic_info.categories.clone(),
            tags: self.basic_info.tags.clone(),
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileEntry {
    async fn system_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.system_time
    }

    async fn event_time(&self) -> &DateTime<Utc> {
        &self.entry.entity.event_time
    }

    async fn version(&self) -> u32 {
        self.entry.entity.version
    }

    async fn content_hash(&self) -> Multihash<'_> {
        Multihash::from(&self.entry.entity.content_hash)
    }

    async fn content_length(&self) -> usize {
        self.entry.entity.content_length
    }

    async fn content_type(&self) -> &String {
        &self.entry.entity.content_type
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
