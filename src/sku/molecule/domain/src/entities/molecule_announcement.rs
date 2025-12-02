// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectAnnouncementDataRecord {
    pub announcement_id: Option<uuid::Uuid>, // Optional for creation

    pub headline: String,

    pub body: String,

    pub attachments: Vec<odf::DatasetID>,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,

    // TODO: enum?
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

pub type MoleculeProjectAnnouncementRecord =
    odf::serde::DatasetDefaultVocabularyRecord<MoleculeProjectAnnouncementDataRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeGlobalAnnouncementDataRecord {
    pub announcement_id: Option<uuid::Uuid>, // Optional for creation

    pub headline: String,

    pub body: String,

    pub attachments: Vec<odf::DatasetID>,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,

    // TODO: enum?
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

pub type MoleculeGlobalAnnouncementRecord =
    odf::serde::DatasetDefaultVocabularyRecord<MoleculeGlobalAnnouncementDataRecord>;

pub trait MoleculeGlobalAnnouncementRecordExt {
    fn as_project_announcement_record(&self) -> MoleculeProjectAnnouncementRecord;
}

impl MoleculeGlobalAnnouncementRecordExt for MoleculeGlobalAnnouncementRecord {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
