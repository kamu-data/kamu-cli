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
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub enum MoleculeDataRoomFileActivityType {
    Added,
    Updated,
    Removed,
}

// TODO: revisit after IPNFT-less projects changes.
#[derive(Debug)]
pub struct MoleculeDataRoomActivityEntity {
    pub system_time: DateTime<Utc>,

    pub event_time: DateTime<Utc>,

    pub activity_type: MoleculeDataRoomFileActivityType,

    pub ipnft_uid: String,

    pub path: String,

    pub r#ref: odf::DatasetID,

    pub version: u32,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    pub change_by: String,

    // TODO: enum?
    pub molecule_access_level: String,

    pub content_type: Option<MediaType>,

    pub content_length: usize,

    pub data_room_dataset_id: odf::DatasetID,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

impl MoleculeDataRoomActivityEntity {
    pub fn from_json(_record: serde_json::Value) -> Result<Self, InternalError> {
        todo!()
    }

    pub fn into_insert_record(self) -> MoleculeDataRoomActivityRecord {
        MoleculeDataRoomActivityRecord {
            system_columns: odf::serde::DatasetDefaultVocabularySystemColumns {
                offset: None,
                op: odf::metadata::OperationType::Append,
                system_time: self.system_time,
                event_time: self.event_time,
            },
            record: MoleculeDataRoomActivityDataRecord {
                ipnft_uid: self.ipnft_uid,
                path: self.path,
                r#ref: self.r#ref,
                version: self.version,
                change_by: self.change_by,
                molecule_access_level: self.molecule_access_level,
                content_type: self.content_type,
                content_length: self.content_length,
                data_room_dataset_id: self.data_room_dataset_id,
                categories: self.categories,
                tags: self.tags,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeDataRoomActivityDataRecord {
    pub ipnft_uid: String,

    pub path: String,

    pub r#ref: odf::DatasetID,

    pub version: u32,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    pub change_by: String,

    // TODO: enum?
    pub molecule_access_level: String,

    pub content_type: Option<MediaType>,

    pub content_length: usize,

    pub data_room_dataset_id: odf::DatasetID,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

pub type MoleculeDataRoomActivityRecord =
    odf::serde::DatasetDefaultVocabularyRecord<MoleculeDataRoomActivityDataRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
