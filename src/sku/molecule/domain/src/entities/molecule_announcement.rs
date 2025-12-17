// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeAnnouncement {
    /// System time when this announcement was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this announcement was created/updated
    pub event_time: DateTime<Utc>,

    pub announcement_id: uuid::Uuid,

    pub headline: String,

    pub body: String,

    pub attachments: Vec<odf::DatasetID>,

    // TODO: enum?
    pub access_level: String,

    // NOTE: This should be odf::AccountID, but kept as String for safety.
    pub change_by: String,

    pub categories: Vec<String>,

    pub tags: Vec<String>,
}

impl MoleculeAnnouncement {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let entry: MoleculeAnnouncementChangelogEntry = serde_json::from_value(record).int_err()?;

        Ok(Self {
            system_time: entry.system_columns.system_time,
            event_time: entry.system_columns.event_time,
            announcement_id: entry.payload.announcement_id,
            headline: entry.payload.headline,
            body: entry.payload.body,
            attachments: entry.payload.attachments,
            access_level: entry.payload.access_level,
            change_by: entry.payload.change_by,
            categories: entry.payload.categories,
            tags: entry.payload.tags,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct MoleculeGlobalAnnouncement {
    pub ipnft_uid: String,
    pub announcement: MoleculeAnnouncement,
}

impl MoleculeGlobalAnnouncement {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let entry: MoleculeGlobalAnnouncementChangelogEntry =
            serde_json::from_value(record).int_err()?;

        Ok(Self {
            ipnft_uid: entry.payload.ipnft_uid,
            announcement: MoleculeAnnouncement {
                system_time: entry.system_columns.system_time,
                event_time: entry.system_columns.event_time,
                announcement_id: entry.payload.announcement.announcement_id,
                headline: entry.payload.announcement.headline,
                body: entry.payload.announcement.body,
                attachments: entry.payload.announcement.attachments,
                access_level: entry.payload.announcement.access_level,
                change_by: entry.payload.announcement.change_by,
                categories: entry.payload.announcement.categories,
                tags: entry.payload.announcement.tags,
            },
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeAnnouncementChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeAnnouncementPayloadRecord>;

pub type MoleculeAnnouncementChangelogInsertionRecord =
    odf::serde::DatasetDefaultVocabularyChangelogInsertionRecord<MoleculeAnnouncementPayloadRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeGlobalAnnouncementPayloadRecord {
    pub ipnft_uid: String,

    #[serde(flatten)]
    pub announcement: MoleculeAnnouncementPayloadRecord,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeGlobalAnnouncementChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeGlobalAnnouncementPayloadRecord>;

pub type MoleculeGlobalAnnouncementChangelogInsertionRecord =
    odf::serde::DatasetDefaultVocabularyChangelogInsertionRecord<
        MoleculeGlobalAnnouncementPayloadRecord,
    >;

pub trait MoleculeGlobalAnnouncementChangelogInsertionRecordExt {
    fn into_announcement_record(self) -> MoleculeAnnouncementChangelogInsertionRecord;
}

impl MoleculeGlobalAnnouncementChangelogInsertionRecordExt
    for MoleculeGlobalAnnouncementChangelogInsertionRecord
{
    fn into_announcement_record(self) -> MoleculeAnnouncementChangelogInsertionRecord {
        MoleculeAnnouncementChangelogInsertionRecord {
            op: self.op,
            payload: self.payload.announcement,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeAnnouncementPayloadRecord {
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

#[derive(Debug)]
pub struct MoleculeAnnouncementsFilters {
    pub by_tags: Option<Vec<String>>,
    pub by_categories: Option<Vec<String>>,

    // NOTE: Access level is required to prevent accidental permission escalations
    pub by_access_levels: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
