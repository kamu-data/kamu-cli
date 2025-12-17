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

// TODO: revisit after IPNFT-less projects changes.
#[derive(Debug, Clone)]
pub struct MoleculeProject {
    /// System time when this project was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this project was created/updated
    pub event_time: DateTime<Utc>,

    /// Account ID associated with this project
    pub account_id: odf::AccountID,

    /// Dataset ID for the data room
    pub data_room_dataset_id: odf::DatasetID,

    /// Dataset ID for announcements
    pub announcements_dataset_id: odf::DatasetID,

    /// Symbolic name of the project
    pub ipnft_symbol: String,

    // TODO: typing
    /// Unique ID of the IPNFT as `{ipnftAddress}_{ipnftTokenId}`
    pub ipnft_uid: String,

    /// Address of the IPNFT contract
    pub ipnft_address: String,

    // NOTE: For backward compatibility (and existing projects),
    //       we continue using BigInt type, which is wider than needed U256.
    /// Token ID withing the IPNFT contract
    pub ipnft_token_id: num_bigint::BigInt,
}

impl MoleculeProject {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let entry: MoleculeProjectChangelogEntry = serde_json::from_value(record).int_err()?;

        let ipnft_token_id = entry
            .payload
            .ipnft_token_id
            .parse()
            .map_err(|e| InternalError::new(format!("Invalid BigInt: {e}")))?;

        Ok(Self {
            system_time: entry.system_columns.timestamp_columns.system_time,
            event_time: entry.system_columns.timestamp_columns.event_time,
            ipnft_symbol: entry.payload.ipnft_symbol,
            ipnft_uid: entry.payload.ipnft_uid,
            ipnft_address: entry.payload.ipnft_address,
            ipnft_token_id,
            account_id: entry.payload.account_id,
            data_room_dataset_id: entry.payload.data_room_dataset_id,
            announcements_dataset_id: entry.payload.announcements_dataset_id,
        })
    }

    pub fn from_payload(
        payload: MoleculeProjectPayloadRecord,
        system_time: DateTime<Utc>,
        event_time: DateTime<Utc>,
    ) -> Result<Self, InternalError> {
        let ipnft_token_id = payload
            .ipnft_token_id
            .parse()
            .map_err(|e| InternalError::new(format!("Invalid BigInt: {e}")))?;

        Ok(Self {
            system_time,
            event_time,
            ipnft_symbol: payload.ipnft_symbol,
            ipnft_uid: payload.ipnft_uid,
            ipnft_address: payload.ipnft_address,
            ipnft_token_id,
            account_id: payload.account_id,
            data_room_dataset_id: payload.data_room_dataset_id,
            announcements_dataset_id: payload.announcements_dataset_id,
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
    pub ipnft_symbol: String,

    pub ipnft_uid: String,

    pub ipnft_address: String,

    pub ipnft_token_id: String,

    pub account_id: odf::AccountID,

    pub data_room_dataset_id: odf::DatasetID,

    pub announcements_dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
