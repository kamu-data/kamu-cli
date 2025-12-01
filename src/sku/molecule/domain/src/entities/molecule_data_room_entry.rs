// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{CollectionEntry, FileVersion};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeDataRoomEntry {
    pub entry: CollectionEntry,
    pub denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom,
}

impl MoleculeDataRoomEntry {
    pub fn try_from_collection_entry(mut entry: CollectionEntry) -> Result<Self, InternalError> {
        let mut extra_data = kamu_datasets::ExtraDataFields::default();
        std::mem::swap(&mut entry.extra_data, &mut extra_data);

        let denormalized_latest_file_info: MoleculeDenormalizeFileToDataRoom =
            serde_json::from_value(extra_data.into_inner().into()).int_err()?;

        Ok(Self {
            entry,
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
