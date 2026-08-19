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
    // Common fields
    pub encrypted_by: String,
    pub encrypted_at: String,
    pub access_control_conditions: String,
    pub encryption_system: Option<String>,

    // Lit-specific
    pub data_to_encrypt_hash: Option<String>,
    pub chain: Option<String>,
    pub lit_sdk_version: Option<String>,
    pub lit_network: Option<String>,
    pub template_name: Option<String>,
    pub contract_version: Option<String>,

    // Envelope encryption fields
    pub encrypted_dek: Option<String>,
    pub iv: Option<String>,
    pub content_hash: Option<String>,
    pub key_id: Option<String>,
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

    // Common fields
    pub encrypted_by: String,
    pub encrypted_at: String,
    pub access_control_conditions: String,
    pub encryption_system: Option<String>,

    // Lit-specific
    pub data_to_encrypt_hash: Option<String>,
    pub chain: Option<String>,
    pub lit_sdk_version: Option<String>,
    pub lit_network: Option<String>,
    pub template_name: Option<String>,
    pub contract_version: Option<String>,

    // Envelope encryption fields
    pub encrypted_dek: Option<String>,
    pub iv: Option<String>,
    pub content_hash: Option<String>,
    pub key_id: Option<String>,
}

impl MoleculeEncryptionMetadataRecord {
    pub fn new(metadata: MoleculeEncryptionMetadata) -> Self {
        Self {
            version: 0,
            encrypted_by: metadata.encrypted_by,
            encrypted_at: metadata.encrypted_at,
            access_control_conditions: metadata.access_control_conditions,
            encryption_system: metadata.encryption_system,
            data_to_encrypt_hash: metadata.data_to_encrypt_hash,
            chain: metadata.chain,
            lit_sdk_version: metadata.lit_sdk_version,
            lit_network: metadata.lit_network,
            template_name: metadata.template_name,
            contract_version: metadata.contract_version,
            encrypted_dek: metadata.encrypted_dek,
            iv: metadata.iv,
            content_hash: metadata.content_hash,
            key_id: metadata.key_id,
        }
    }

    pub fn as_entity(&self) -> MoleculeEncryptionMetadata {
        MoleculeEncryptionMetadata {
            encrypted_by: self.encrypted_by.clone(),
            encrypted_at: self.encrypted_at.clone(),
            access_control_conditions: self.access_control_conditions.clone(),
            encryption_system: self.encryption_system.clone(),
            data_to_encrypt_hash: self.data_to_encrypt_hash.clone(),
            chain: self.chain.clone(),
            lit_sdk_version: self.lit_sdk_version.clone(),
            lit_network: self.lit_network.clone(),
            template_name: self.template_name.clone(),
            contract_version: self.contract_version.clone(),
            encrypted_dek: self.encrypted_dek.clone(),
            iv: self.iv.clone(),
            content_hash: self.content_hash.clone(),
            key_id: self.key_id.clone(),
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

        // Common fields
        pub encrypted_by: String,
        pub encrypted_at: String,
        pub access_control_conditions: String,
        pub encryption_system: Option<String>,

        // Lit-specific
        pub data_to_encrypt_hash: Option<String>,
        pub chain: Option<String>,
        pub lit_sdk_version: Option<String>,
        pub lit_network: Option<String>,
        pub template_name: Option<String>,
        pub contract_version: Option<String>,

        // Envelope encryption fields
        pub encrypted_dek: Option<String>,
        pub iv: Option<String>,
        pub content_hash: Option<String>,
        pub key_id: Option<String>,
    }

    impl serde::Serialize for MoleculeEncryptionMetadataRecord {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            let helper = Helper {
                version: self.version,
                encrypted_by: self.encrypted_by.clone(),
                encrypted_at: self.encrypted_at.clone(),
                access_control_conditions: self.access_control_conditions.clone(),
                encryption_system: self.encryption_system.clone(),
                data_to_encrypt_hash: self.data_to_encrypt_hash.clone(),
                chain: self.chain.clone(),
                lit_sdk_version: self.lit_sdk_version.clone(),
                lit_network: self.lit_network.clone(),
                template_name: self.template_name.clone(),
                contract_version: self.contract_version.clone(),
                encrypted_dek: self.encrypted_dek.clone(),
                iv: self.iv.clone(),
                content_hash: self.content_hash.clone(),
                key_id: self.key_id.clone(),
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
                encrypted_by: helper.encrypted_by,
                encrypted_at: helper.encrypted_at,
                access_control_conditions: helper.access_control_conditions,
                encryption_system: helper.encryption_system,
                data_to_encrypt_hash: helper.data_to_encrypt_hash,
                chain: helper.chain,
                lit_sdk_version: helper.lit_sdk_version,
                lit_network: helper.lit_network,
                template_name: helper.template_name,
                contract_version: helper.contract_version,
                encrypted_dek: helper.encrypted_dek,
                iv: helper.iv,
                content_hash: helper.content_hash,
                key_id: helper.key_id,
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
