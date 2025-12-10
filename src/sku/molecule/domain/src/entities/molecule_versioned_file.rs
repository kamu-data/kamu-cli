// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use chrono::{DateTime, Utc};
use file_utils::MediaType;
use kamu_datasets::FileVersion;

use crate::{MoleculeDenormalizeFileToDataRoom, MoleculeEncryptionMetadataRecord};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileEntry {
    /// System time when this entry was created
    pub system_time: DateTime<Utc>,

    /// Event time when this entry was created
    pub event_time: DateTime<Utc>,

    /// File version
    pub version: FileVersion,

    /// Media type of the file content
    pub content_type: MediaType,

    /// Size of the content in bytes
    pub content_length: usize,

    /// Multihash of the file content
    pub content_hash: odf::Multihash,

    /// Basic versioned file info
    pub basic_info: MoleculeVersionedFileEntryBasicInfo,

    /// Detailed versioned file info
    pub detailed_info: MoleculeVersionedFileEntryDetailedInfo,
}

impl MoleculeVersionedFileEntry {
    pub fn from_raw_versioned_file_entry(raw: kamu_datasets::VersionedFileEntry) -> Self {
        let extra_data = MoleculeVersionedFileExtraData::from_extra_data_fields(raw.extra_data);

        MoleculeVersionedFileEntry {
            system_time: raw.system_time,
            event_time: raw.event_time,
            version: raw.version,
            content_type: raw.content_type,
            content_length: raw.content_length,
            content_hash: raw.content_hash,
            basic_info: extra_data.basic_info.into_owned(),
            detailed_info: extra_data.detailed_info.into_owned(),
        }
    }

    pub fn to_versioned_file_extra_data(&self) -> kamu_datasets::ExtraDataFields {
        let extra_data = MoleculeVersionedFileExtraData {
            basic_info: Cow::Borrowed(&self.basic_info),
            detailed_info: Cow::Borrowed(&self.detailed_info),
        };

        let serde_json::Value::Object(json) = serde_json::to_value(&extra_data).unwrap() else {
            unreachable!()
        };

        kamu_datasets::ExtraDataFields::new(json)
    }

    pub fn to_denormalized(&self) -> MoleculeDenormalizeFileToDataRoom {
        MoleculeDenormalizeFileToDataRoom {
            access_level: self.basic_info.access_level.clone(),
            change_by: self.basic_info.change_by.clone(),
            version: self.version,
            content_type: self.content_type.clone(),
            content_length: self.content_length,
            content_hash: self.content_hash.clone(),
            description: self.basic_info.description.clone(),
            categories: self.basic_info.categories.clone(),
            tags: self.basic_info.tags.clone(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MoleculeVersionedFileEntryBasicInfo {
    #[serde(rename = "molecule_access_level")]
    pub access_level: String,
    #[serde(rename = "molecule_change_by")]
    pub change_by: String,
    pub description: Option<String>,
    pub categories: Vec<String>,
    pub tags: Vec<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MoleculeVersionedFileEntryDetailedInfo {
    pub content_text: Option<String>,
    pub encryption_metadata: Option<MoleculeEncryptionMetadataRecord>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct MoleculeVersionedFileExtraData<'a> {
    #[serde(flatten)]
    pub basic_info: Cow<'a, MoleculeVersionedFileEntryBasicInfo>,

    #[serde(flatten)]
    pub detailed_info: Cow<'a, MoleculeVersionedFileEntryDetailedInfo>,
}

impl Default for MoleculeVersionedFileExtraData<'_> {
    fn default() -> Self {
        Self {
            basic_info: Cow::Owned(MoleculeVersionedFileEntryBasicInfo::default()),
            detailed_info: Cow::Owned(MoleculeVersionedFileEntryDetailedInfo::default()),
        }
    }
}

impl MoleculeVersionedFileExtraData<'static> {
    pub fn from_extra_data_fields(extra_data: kamu_datasets::ExtraDataFields) -> Self {
        serde_json::from_value(serde_json::Value::Object(extra_data.into_inner()))
            .unwrap_or_default()
    }
}

impl MoleculeVersionedFileExtraData<'_> {
    pub fn to_extra_data_fields(&self) -> kamu_datasets::ExtraDataFields {
        let serde_json::Value::Object(json) = serde_json::to_value(self).unwrap() else {
            unreachable!()
        };
        kamu_datasets::ExtraDataFields::new(json)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
