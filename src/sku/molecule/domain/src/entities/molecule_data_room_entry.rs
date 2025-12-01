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
use kamu_datasets::{CollectionEntry, CollectionPath, FileVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct MoleculeDataRoomEntry {
    /// System time when this entry was created
    pub system_time: DateTime<Utc>,

    /// Event time when this entry was created
    pub event_time: DateTime<Utc>,

    /// File system-like path
    /// Rooted, separated by forward slashes, with elements URL-encoded
    /// (e.g. `/foo%20bar/baz`)
    pub path: CollectionPath,

    /// DID of the linked dataset
    pub reference: odf::DatasetID,

    /// Denormalized info about the linked file
    pub denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
}

impl MoleculeDataRoomEntry {
    pub fn try_from_collection_entry(entry: CollectionEntry) -> Result<Self, InternalError> {
        let denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom =
            serde_json::from_value(entry.extra_data.into_inner().into()).int_err()?;

        Ok(Self {
            system_time: entry.system_time,
            event_time: entry.event_time,
            path: entry.path,
            reference: entry.reference,
            denormalized_latest_file_info,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// These fields are stored as extra columns in data room collection
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeDenormalizeFileToDataRoom {
    pub version: FileVersion,
    pub content_type: String,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
