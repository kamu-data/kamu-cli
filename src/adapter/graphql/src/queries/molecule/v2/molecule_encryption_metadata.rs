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
    async fn data_to_encrypt_hash(&self) -> &String {
        &self.entity.data_to_encrypt_hash
    }

    async fn access_control_conditions(&self) -> &String {
        &self.entity.access_control_conditions
    }

    async fn encrypted_by(&self) -> &String {
        &self.entity.encrypted_by
    }

    async fn encrypted_at(&self) -> &String {
        &self.entity.encrypted_at
    }

    async fn chain(&self) -> &String {
        &self.entity.chain
    }

    async fn lit_sdk_version(&self) -> &String {
        &self.entity.lit_sdk_version
    }

    async fn lit_network(&self) -> &String {
        &self.entity.lit_network
    }

    async fn template_name(&self) -> &String {
        &self.entity.template_name
    }

    async fn contract_version(&self) -> &String {
        &self.entity.contract_version
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

impl From<MoleculeEncryptionMetadataInput> for kamu_molecule_domain::MoleculeEncryptionMetadata {
    fn from(input: MoleculeEncryptionMetadataInput) -> Self {
        Self {
            data_to_encrypt_hash: input.data_to_encrypt_hash,
            access_control_conditions: input.access_control_conditions,
            encrypted_by: input.encrypted_by,
            encrypted_at: input.encrypted_at,
            chain: input.chain,
            lit_sdk_version: input.lit_sdk_version,
            lit_network: input.lit_network,
            template_name: input.template_name,
            contract_version: input.contract_version,
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
