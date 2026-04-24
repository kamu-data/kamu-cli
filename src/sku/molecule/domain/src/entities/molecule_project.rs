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

// TODO: Molecule: Phase 3: is this TODO still needed?
// TODO: revisit after IPNFT-less projects changes.
#[derive(Debug, Clone)]
pub struct MoleculeProject {
    /// System time when this project was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this project was created/updated
    pub event_time: DateTime<Utc>,

    // TODO: Molecule: Phase 3: stack-based type? oclId is bytes32 in Solidity terms
    /// Unique ID: OCL (On-Chain Labs)
    pub ocl_id: String,

    /// Symbolic name of the project
    pub symbol: String,

    /// Account ID associated with this project
    pub account_id: odf::AccountID,

    /// Dataset ID for the data room
    pub data_room_dataset_id: odf::DatasetID,

    /// Dataset ID for announcements
    pub announcements_dataset_id: odf::DatasetID,
}

impl MoleculeProject {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let entry: MoleculeProjectChangelogEntry = serde_json::from_value(record).int_err()?;

        Ok(Self {
            system_time: entry.system_columns.timestamp_columns.system_time,
            event_time: entry.system_columns.timestamp_columns.event_time,
            ocl_id: entry.payload.ocl_id,
            symbol: entry.payload.symbol,
            account_id: entry.payload.odf_account_id,
            data_room_dataset_id: entry.payload.odf_data_room_dataset_id,
            announcements_dataset_id: entry.payload.odf_announcements_dataset_id,
        })
    }

    pub fn from_payload(
        payload: MoleculeProjectPayloadRecord,
        system_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
    ) -> Result<Self, InternalError> {
        Ok(Self {
            system_time,
            event_time,
            ocl_id: payload.ocl_id,
            symbol: payload.symbol,
            account_id: payload.odf_account_id,
            data_room_dataset_id: payload.odf_data_room_dataset_id,
            announcements_dataset_id: payload.odf_announcements_dataset_id,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeProjectChangelogEntry =
    odf::serde::DatasetDefaultVocabularyChangelogEntry<MoleculeProjectPayloadRecord>;

pub type MoleculeProjectChangelogInsertionRecord =
    odf::serde::DatasetDefaultVocabularyChangelogInsertionRecord<MoleculeProjectPayloadRecord>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectPayloadRecord {
    pub ocl_id: String,
    pub symbol: String,
    pub odf_account_id: odf::AccountID,
    pub odf_data_room_dataset_id: odf::DatasetID,
    pub odf_announcements_dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
