// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: introduce Entity (based on composition), e.g.:
// pub struct AnnouncementEntity {
//    pub announcement_id: String,
//    pub record: DatasetDefaultVocabularyRecord<AnnouncementRecord>,
// }

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectAnnouncementDataRecord {
    pub announcement_id: Option<uuid::Uuid>, // Optional for creation

    pub headline: String,

    pub body: String,

    pub attachments: Vec<odf::DatasetID>,

    // TODO: enum?
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,

    #[serde(default)]
    pub categories: Vec<String>,

    #[serde(default)]
    pub tags: Vec<String>,
}

pub type MoleculeProjectAnnouncementRecord =
    odf::serde::DatasetDefaultVocabularyRecord<MoleculeProjectAnnouncementDataRecord>;

pub trait MoleculeProjectAnnouncementRecordExt {
    fn from_json(
        record: serde_json::Value,
    ) -> Result<MoleculeProjectAnnouncementRecord, InternalError>;
}

impl MoleculeProjectAnnouncementRecordExt for MoleculeProjectAnnouncementRecord {
    fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        serde_json::from_value(record).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeGlobalAnnouncementDataRecord {
    pub announcement_id: Option<uuid::Uuid>, // Optional for creation

    pub ipnft_uid: String,

    pub headline: String,

    pub body: String,

    pub attachments: Vec<odf::DatasetID>,

    // TODO: enum?
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

pub type MoleculeGlobalAnnouncementRecord =
    odf::serde::DatasetDefaultVocabularyRecord<MoleculeGlobalAnnouncementDataRecord>;

pub trait MoleculeGlobalAnnouncementRecordExt {
    fn from_json(
        record: serde_json::Value,
    ) -> Result<MoleculeGlobalAnnouncementRecord, InternalError>;

    fn as_project_announcement_record(&self) -> MoleculeProjectAnnouncementRecord;

    fn into_project_announcement_record(self) -> MoleculeProjectAnnouncementRecord;
}

impl MoleculeGlobalAnnouncementRecordExt for MoleculeGlobalAnnouncementRecord {
    fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        serde_json::from_value(record).int_err()
    }

    fn as_project_announcement_record(&self) -> MoleculeProjectAnnouncementRecord {
        MoleculeProjectAnnouncementRecord {
            system_columns: self.system_columns.clone(),
            record: MoleculeProjectAnnouncementDataRecord {
                announcement_id: self.record.announcement_id,
                headline: self.record.headline.clone(),
                body: self.record.body.clone(),
                attachments: self.record.attachments.clone(),
                change_by: self.record.change_by.clone(),
                access_level: self.record.access_level.clone(),
                categories: self.record.categories.clone(),
                tags: self.record.tags.clone(),
            },
        }
    }

    fn into_project_announcement_record(self) -> MoleculeProjectAnnouncementRecord {
        MoleculeProjectAnnouncementRecord {
            system_columns: self.system_columns,
            record: MoleculeProjectAnnouncementDataRecord {
                announcement_id: self.record.announcement_id,
                headline: self.record.headline,
                body: self.record.body,
                attachments: self.record.attachments,
                change_by: self.record.change_by,
                access_level: self.record.access_level,
                categories: self.record.categories,
                tags: self.record.tags,
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct MoleculeAnnouncementsFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,
    pub by_access_levels: Option<Vec<String>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
