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

pub struct MoleculeEncryptionMetadata {
    entity: kamu_molecule_domain::MoleculeEncryptionMetadata,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeEncryptionMetadata {
    // Common fields

    async fn encrypted_by(&self) -> &String {
        &self.entity.encrypted_by
    }

    async fn encrypted_at(&self) -> &String {
        &self.entity.encrypted_at
    }

    async fn access_control_conditions(&self) -> &String {
        &self.entity.access_control_conditions
    }

    async fn encryption_system(&self) -> Option<&String> {
        self.entity.encryption_system.as_ref()
    }

    // Lit-specific

    async fn data_to_encrypt_hash(&self) -> Option<&String> {
        self.entity.data_to_encrypt_hash.as_ref()
    }

    async fn chain(&self) -> Option<&String> {
        self.entity.chain.as_ref()
    }

    async fn lit_sdk_version(&self) -> Option<&String> {
        self.entity.lit_sdk_version.as_ref()
    }

    async fn lit_network(&self) -> Option<&String> {
        self.entity.lit_network.as_ref()
    }

    async fn template_name(&self) -> Option<&String> {
        self.entity.template_name.as_ref()
    }

    async fn contract_version(&self) -> Option<&String> {
        self.entity.contract_version.as_ref()
    }

    // Envelope encryption fields

    async fn encrypted_dek(&self) -> Option<&String> {
        self.entity.encrypted_dek.as_ref()
    }

    async fn iv(&self) -> Option<&String> {
        self.entity.iv.as_ref()
    }

    async fn content_hash(&self) -> Option<&String> {
        self.entity.content_hash.as_ref()
    }

    async fn key_id(&self) -> Option<&String> {
        self.entity.key_id.as_ref()
    }
}

impl From<kamu_molecule_domain::MoleculeEncryptionMetadata> for MoleculeEncryptionMetadata {
    fn from(metadata: kamu_molecule_domain::MoleculeEncryptionMetadata) -> Self {
        Self { entity: metadata }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(InputObject)]
pub struct MoleculeEncryptionMetadataInput {
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

impl From<MoleculeEncryptionMetadataInput> for kamu_molecule_domain::MoleculeEncryptionMetadata {
    fn from(input: MoleculeEncryptionMetadataInput) -> Self {
        Self {
            encrypted_by: input.encrypted_by,
            encrypted_at: input.encrypted_at,
            access_control_conditions: input.access_control_conditions,
            encryption_system: input.encryption_system,
            data_to_encrypt_hash: input.data_to_encrypt_hash,
            chain: input.chain,
            lit_sdk_version: input.lit_sdk_version,
            lit_network: input.lit_network,
            template_name: input.template_name,
            contract_version: input.contract_version,
            encrypted_dek: input.encrypted_dek,
            iv: input.iv,
            content_hash: input.content_hash,
            key_id: input.key_id,
        }
    }
}

impl From<MoleculeEncryptionMetadataInput>
    for kamu_molecule_domain::MoleculeEncryptionMetadataRecord
{
    fn from(input: MoleculeEncryptionMetadataInput) -> Self {
        let metadata: kamu_molecule_domain::MoleculeEncryptionMetadata = input.into();
        Self::new(metadata)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
