// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use file_utils::MediaType;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{CollectionEntry, CollectionPathV2, FileVersion};

use crate::MoleculeAccessLevelRule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomEntry {
    /// System time when this entry was created
    pub system_time: DateTime<Utc>,

    /// Event time when this entry was created
    pub event_time: DateTime<Utc>,

    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    pub path: CollectionPathV2,

    /// DID of the linked dataset
    pub reference: odf::DatasetID,

    /// Denormalized info about the linked file
    pub denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
}

impl MoleculeDataRoomEntry {
    pub fn from_collection_entry(entry: CollectionEntry) -> Self {
        let denormalized_latest_file_info =
            MoleculeDenormalizeFileToDataRoom::try_from_extra_data_fields(entry.extra_data)
                .ok()
                .unwrap_or_default();

        Self {
            system_time: entry.system_time,
            event_time: entry.event_time,
            // SAFETY: All paths should be normalized after v2 migration
            path: CollectionPathV2::from_v1_unchecked(entry.path),
            reference: entry.reference,
            denormalized_latest_file_info,
        }
    }

    pub fn from_changelog_entry_json(
        mut value: serde_json::Value,
        vocab: &odf::metadata::DatasetVocabulary,
    ) -> Result<(u64, odf::metadata::OperationType, Self), InternalError> {
        let Some(obj) = value.as_object_mut() else {
            unreachable!()
        };
        let Some(raw_op) = obj[&vocab.operation_type_column].as_i64() else {
            unreachable!()
        };
        let Some(offset) = obj[&vocab.offset_column].as_u64() else {
            unreachable!()
        };

        let op =
            odf::metadata::OperationType::try_from(u8::try_from(raw_op).int_err()?).int_err()?;

        let collection_entity = kamu_datasets::CollectionEntry::from_json(value).int_err()?;
        let data_room_entry = Self::from_collection_entry(collection_entity);

        Ok((offset, op, data_room_entry))
    }

    pub fn is_only_change_by_diff(&self, other: &Self) -> bool {
        if self.path != other.path || self.reference != other.reference {
            return false;
        }

        let lhs = &self.denormalized_latest_file_info;
        let rhs = &other.denormalized_latest_file_info;

        lhs.version == rhs.version
            && lhs.content_type == rhs.content_type
            && lhs.content_length == rhs.content_length
            && lhs.content_hash == rhs.content_hash
            && lhs.access_level == rhs.access_level
            && lhs.description == rhs.description
            && lhs.categories == rhs.categories
            && lhs.tags == rhs.tags
            && lhs.change_by != rhs.change_by
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// These fields are stored as extra columns in data room collection
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDenormalizeFileToDataRoom {
    pub version: FileVersion,
    pub content_type: MediaType,
    pub content_length: usize,
    pub content_hash: odf::Multihash,

    #[serde(rename = "molecule_access_level")]
    pub access_level: String,
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,
    pub description: Option<String>,
    pub categories: Vec<String>,
    pub tags: Vec<String>,
}

impl Default for MoleculeDenormalizeFileToDataRoom {
    fn default() -> Self {
        Self {
            version: 0,
            content_type: MediaType::OCTET_STREAM.to_owned(),
            content_length: 0,
            content_hash: odf::Multihash::from_digest_sha3_256(b""),
            access_level: String::new(),
            change_by: String::new(),
            description: None,
            categories: Vec::new(),
            tags: Vec::new(),
        }
    }
}

impl MoleculeDenormalizeFileToDataRoom {
    pub fn try_from_extra_data_fields(
        extra_data: kamu_datasets::ExtraDataFields,
    ) -> Result<Self, serde_json::Error> {
        serde_json::from_value(extra_data.into_inner().into())
    }

    pub fn to_collection_extra_data_fields(&self) -> kamu_datasets::ExtraDataFields {
        let serde_json::Value::Object(json_map) = serde_json::to_value(self).unwrap() else {
            unreachable!()
        };

        kamu_datasets::ExtraDataFields::new(json_map)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeDataRoomEntriesFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
    pub by_access_level_rules: Option<Vec<MoleculeAccessLevelRule>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
