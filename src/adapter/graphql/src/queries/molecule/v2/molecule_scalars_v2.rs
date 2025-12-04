// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: use enum instead of string?
// #[derive(Enum)]
// pub enum MoleculeAccessLevelV2 {
//     Public,
//     Admin,
//     Admin2,
//     Holder,
// }

pub type MoleculeAccessLevel = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type MoleculeChangeBy = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type MoleculeAnnouncementId = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type MoleculeCategory = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: scalar?
pub type MoleculeTag = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: break into scalar (w/o serde) & domain struct (w/ serde) -->
// TODO: clarify valid values
#[derive(Clone, Debug, InputObject, serde::Deserialize, serde::Serialize, SimpleObject)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
#[graphql(input_name = "MoleculeEncryptionMetadataInput")]
pub struct MoleculeEncryptionMetadata {
    pub data_to_encrypt_hash: String,
    pub access_control_conditions: String,
    pub encrypted_by: String,
    pub encrypted_at: String,
    pub chain: String,
    pub lit_sdk_version: String,
    pub lit_network: String,
    pub template_name: String,
    pub contract_version: String,
}

// NOTE: structure used for de/serialization (as a JSON string)
//       and storage in versioned file datasets
#[derive(Clone, Debug)]
pub struct MoleculeEncryptionMetadataRecord {
    // Allow for future record evolution
    pub version: u32,
    pub record: MoleculeEncryptionMetadata,
}

impl MoleculeEncryptionMetadataRecord {
    pub fn new(record: MoleculeEncryptionMetadata) -> Self {
        Self { version: 0, record }
    }
}

mod encryption_metadata_record_serde {
    use super::{MoleculeEncryptionMetadata, MoleculeEncryptionMetadataRecord};

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Helper {
        version: u32,
        #[serde(flatten)]
        record: MoleculeEncryptionMetadata,
    }

    impl serde::Serialize for MoleculeEncryptionMetadataRecord {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let helper = Helper {
                version: self.version,
                record: self.record.clone(),
            };
            let raw_json_as_str =
                serde_json::to_string(&helper).map_err(serde::ser::Error::custom)?;

            serializer.serialize_str(&raw_json_as_str)
        }
    }

    impl<'de> serde::Deserialize<'de> for MoleculeEncryptionMetadataRecord {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let raw_json_as_str = String::deserialize(deserializer)?;
            let helper: Helper =
                serde_json::from_str(&raw_json_as_str).map_err(serde::de::Error::custom)?;

            Ok(Self {
                version: helper.version,
                record: helper.record,
            })
        }
    }
}
// <-- TODO: break into scalar (w/o serde) & domain struct (w/ serde)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
