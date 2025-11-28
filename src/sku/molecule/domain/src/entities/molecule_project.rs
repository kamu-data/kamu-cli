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
pub struct MoleculeProjectEntity {
    /// Account ID associated with this project
    pub account_id: odf::AccountID,

    /// Dataset ID for the data room
    pub data_room_dataset_id: odf::DatasetID,

    /// Dataset ID for announcements
    pub announcements_dataset_id: odf::DatasetID,

    /// System time when this project was created/updated
    pub system_time: DateTime<Utc>,

    /// Event time when this project was created/updated
    pub event_time: DateTime<Utc>,

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

impl MoleculeProjectEntity {
    pub fn from_json(record: serde_json::Value) -> Result<Self, InternalError> {
        let record: MoleculeProjectChangelogRecord = serde_json::from_value(record).int_err()?;

        let ipnft_token_id = record
            .data
            .ipnft_token_id
            .parse()
            .map_err(|e| InternalError::new(format!("Invalid BigInt: {e}")))?;

        Ok(Self {
            system_time: record.system_time,
            event_time: record.event_time,
            ipnft_symbol: record.data.ipnft_symbol,
            ipnft_uid: record.data.ipnft_uid,
            ipnft_address: record.data.ipnft_address,
            ipnft_token_id,
            account_id: record.data.account_id,
            data_room_dataset_id: record.data.data_room_dataset_id,
            announcements_dataset_id: record.data.announcements_dataset_id,
        })
    }

    pub fn as_changelog_record(&self, op: u8) -> MoleculeProjectChangelogRecord {
        MoleculeProjectChangelogRecord {
            offset: None,
            op,
            system_time: self.system_time,
            event_time: self.event_time,
            data: MoleculeProjectDataRecord {
                ipnft_symbol: self.ipnft_symbol.clone(),
                ipnft_uid: self.ipnft_uid.clone(),
                ipnft_address: self.ipnft_address.clone(),
                ipnft_token_id: self.ipnft_token_id.to_string(),
                account_id: self.account_id.clone(),
                data_room_dataset_id: self.data_room_dataset_id.clone(),
                announcements_dataset_id: self.announcements_dataset_id.clone(),
            },
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectChangelogRecord {
    pub offset: Option<u64>, // Optional for creation

    pub op: u8,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub system_time: DateTime<Utc>,

    #[serde(with = "odf::serde::yaml::datetime_rfc3339")]
    pub event_time: DateTime<Utc>,

    #[serde(flatten)]
    pub data: MoleculeProjectDataRecord,
}

impl MoleculeProjectChangelogRecord {
    pub fn to_bytes(&self) -> bytes::Bytes {
        let json = serde_json::to_string(self).unwrap();
        bytes::Bytes::from_owner(json.into_bytes())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct MoleculeProjectDataRecord {
    pub ipnft_symbol: String,

    pub ipnft_uid: String,

    pub ipnft_address: String,

    pub ipnft_token_id: String,

    pub account_id: odf::AccountID,

    pub data_room_dataset_id: odf::DatasetID,

    pub announcements_dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
