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

use crate::MoleculeDataRoomEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Copy, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MoleculeDataRoomFileActivityType {
    Added,
    Updated,
    Removed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: revisit after IPNFT-less projects changes.
#[derive(Debug)]
pub struct MoleculeDataRoomActivity {
    pub offset: u64, // We need this offset for identification

    pub system_time: DateTime<Utc>,

    pub event_time: DateTime<Utc>,

    pub activity_type: MoleculeDataRoomFileActivityType,

    // TODO: typing
    pub ipnft_uid: String,

    pub path: kamu_datasets::CollectionPath,

    pub r#ref: odf::DatasetID,

    pub version: u32,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    pub change_by: String,

    // TODO: Communicate: we need to agree on its values
    pub access_level: String,

    pub content_type: Option<MediaType>,

    pub content_length: usize,

    pub content_hash: odf::Multihash,

    pub description: Option<String>,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

impl MoleculeDataRoomActivity {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let r: MoleculeDataRoomActivityChangelogEntry = serde_json::from_value(record).int_err()?;

        Ok(Self {
            offset: r.system_columns.offset,
            system_time: r.system_columns.timestamp_columns.system_time,
            event_time: r.system_columns.timestamp_columns.event_time,
            activity_type: r.payload.activity_type,
            ipnft_uid: r.payload.ipnft_uid,
            path: r.payload.path,
            r#ref: r.payload.r#ref,
            version: r.payload.version,
            change_by: r.payload.change_by,
            access_level: r.payload.access_level,
            content_type: r.payload.content_type,
            content_length: r.payload.content_length,
            content_hash: r.payload.content_hash,
            description: r.payload.description,
            categories: r.payload.categories,
            tags: r.payload.tags,
        })
    }

    pub fn from_data_room_operation(
        offset: u64,
        activity_type: MoleculeDataRoomFileActivityType,
        entry: MoleculeDataRoomEntry,
        ipnft_uid: String,
    ) -> Self {
        Self {
            offset,
            system_time: entry.system_time,
            event_time: entry.event_time,
            activity_type,
            ipnft_uid,
            path: entry.path,
            r#ref: entry.reference,
            version: entry.denormalized_latest_file_info.version,
            change_by: entry.denormalized_latest_file_info.change_by,
            access_level: entry.denormalized_latest_file_info.access_level,
            content_type: Some(entry.denormalized_latest_file_info.content_type),
            content_length: entry.denormalized_latest_file_info.content_length,
            content_hash: entry.denormalized_latest_file_info.content_hash,
            description: entry.denormalized_latest_file_info.description,
            categories: entry.denormalized_latest_file_info.categories,
            tags: entry.denormalized_latest_file_info.tags,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomActivityPayloadRecord {
    pub activity_type: MoleculeDataRoomFileActivityType,

    pub ipnft_uid: String,

    pub path: kamu_datasets::CollectionPath,

    pub r#ref: odf::DatasetID,

    pub version: u32,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,

    // TODO: enum?
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,

    pub content_type: Option<MediaType>,

    pub content_length: usize,

    pub content_hash: odf::Multihash,

    pub description: Option<String>,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeDataRoomActivityChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeDataRoomActivityPayloadRecord>;

pub type MoleculeDataRoomActivityChangelogInsertionRecord =
    odf::serde::DatasetDefaultVocabularyChangelogInsertionRecord<
        MoleculeDataRoomActivityPayloadRecord,
    >;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
