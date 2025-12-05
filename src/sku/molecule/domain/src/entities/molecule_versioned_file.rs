// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: clarify valid values
#[derive(Clone, Debug)]
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

impl MoleculeEncryptionMetadata {
    pub fn into_record(self) -> MoleculeEncryptionMetadataRecord {
        MoleculeEncryptionMetadataRecord::new(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// NOTE: structure used for de/serialization
//       (as a JSON string, not a JSON object)
//       and storage in versioned file datasets in (!) string column
#[derive(Clone, Debug)]
pub struct MoleculeEncryptionMetadataRecord {
    // Allow for future record evolution
    pub version: u32,

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

impl MoleculeEncryptionMetadataRecord {
    pub fn new(metadata: MoleculeEncryptionMetadata) -> Self {
        Self {
            version: 0,
            data_to_encrypt_hash: metadata.data_to_encrypt_hash,
            access_control_conditions: metadata.access_control_conditions,
            encrypted_by: metadata.encrypted_by,
            encrypted_at: metadata.encrypted_at,
            chain: metadata.chain,
            lit_sdk_version: metadata.lit_sdk_version,
            lit_network: metadata.lit_network,
            template_name: metadata.template_name,
            contract_version: metadata.contract_version,
        }
    }

    pub fn as_entity(&self) -> MoleculeEncryptionMetadata {
        MoleculeEncryptionMetadata {
            data_to_encrypt_hash: self.data_to_encrypt_hash.clone(),
            access_control_conditions: self.access_control_conditions.clone(),
            encrypted_by: self.encrypted_by.clone(),
            encrypted_at: self.encrypted_at.clone(),
            chain: self.chain.clone(),
            lit_sdk_version: self.lit_sdk_version.clone(),
            lit_network: self.lit_network.clone(),
            template_name: self.template_name.clone(),
            contract_version: self.contract_version.clone(),
        }
    }
}

mod encryption_metadata_record_serde {
    use super::MoleculeEncryptionMetadataRecord;

    // Helper structure has regular de/serialization
    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Helper {
        version: u32,
        data_to_encrypt_hash: String,
        access_control_conditions: String,
        encrypted_by: String,
        encrypted_at: String,
        chain: String,
        lit_sdk_version: String,
        lit_network: String,
        template_name: String,
        contract_version: String,
    }

    impl serde::Serialize for MoleculeEncryptionMetadataRecord {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let helper = Helper {
                version: self.version,
                data_to_encrypt_hash: self.data_to_encrypt_hash.clone(),
                access_control_conditions: self.access_control_conditions.clone(),
                encrypted_by: self.encrypted_by.clone(),
                encrypted_at: self.encrypted_at.clone(),
                chain: self.chain.clone(),
                lit_sdk_version: self.lit_sdk_version.clone(),
                lit_network: self.lit_network.clone(),
                template_name: self.template_name.clone(),
                contract_version: self.contract_version.clone(),
            };

            let json_value_as_json_string =
                serde_json::to_string(&helper).map_err(serde::ser::Error::custom)?;
            serializer.serialize_str(&json_value_as_json_string)
        }
    }

    impl<'de> serde::Deserialize<'de> for MoleculeEncryptionMetadataRecord {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            let json_value_as_json_string = String::deserialize(deserializer)?;

            let helper: Helper = serde_json::from_str(&json_value_as_json_string)
                .map_err(serde::de::Error::custom)?;

            Ok(Self {
                version: helper.version,
                data_to_encrypt_hash: helper.data_to_encrypt_hash,
                access_control_conditions: helper.access_control_conditions,
                encrypted_by: helper.encrypted_by,
                encrypted_at: helper.encrypted_at,
                chain: helper.chain,
                lit_sdk_version: helper.lit_sdk_version,
                lit_network: helper.lit_network,
                template_name: helper.template_name,
                contract_version: helper.contract_version,
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
