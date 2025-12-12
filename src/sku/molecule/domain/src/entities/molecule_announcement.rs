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

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeAnnouncementRecord {
    pub announcement_id: uuid::Uuid,

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeAnnouncementChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeAnnouncementRecord>;

pub trait MoleculeAnnouncementChangelogEntryExt {
    fn from_json(
        record: serde_json::Value,
    ) -> Result<MoleculeAnnouncementChangelogEntry, InternalError>;
}

impl MoleculeAnnouncementChangelogEntryExt for MoleculeAnnouncementChangelogEntry {
    fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        serde_json::from_value(record).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeGlobalAnnouncementRecord {
    pub ipnft_uid: String,

    #[serde(flatten)]
    pub announcement: MoleculeAnnouncementRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeGlobalAnnouncementChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeGlobalAnnouncementRecord>;

pub trait MoleculeGlobalAnnouncementChangelogEntryExt {
    fn from_json(
        record: serde_json::Value,
    ) -> Result<MoleculeGlobalAnnouncementChangelogEntry, InternalError>;

    fn as_announcement_entry(&self) -> MoleculeAnnouncementChangelogEntry;

    fn into_announcement_entry(self) -> MoleculeAnnouncementChangelogEntry;
}

impl MoleculeGlobalAnnouncementChangelogEntryExt for MoleculeGlobalAnnouncementChangelogEntry {
    fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        serde_json::from_value(record).int_err()
    }

    fn as_announcement_entry(&self) -> MoleculeAnnouncementChangelogEntry {
        MoleculeAnnouncementChangelogEntry {
            system_columns: self.system_columns.clone(),
            record: self.record.announcement.clone(),
        }
    }

    fn into_announcement_entry(self) -> MoleculeAnnouncementChangelogEntry {
        MoleculeAnnouncementChangelogEntry {
            system_columns: self.system_columns,
            record: self.record.announcement,
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
