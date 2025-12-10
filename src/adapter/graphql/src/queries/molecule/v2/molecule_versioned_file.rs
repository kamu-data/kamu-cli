// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use kamu_molecule_domain::{
    MoleculeReadVersionedFileEntryError,
    MoleculeReadVersionedFileEntryUseCase,
    MoleculeVersionedFileContentProvider,
    MoleculeVersionedFileContentProviderError,
};

use crate::prelude::*;
use crate::queries::VersionedFileContentDownload;
use crate::queries::molecule::v2::{MoleculeAccessLevel, MoleculeCategory, MoleculeTag};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// MoleculeVersionedFile
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFile {
    pub dataset_id: odf::DatasetID,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFile {
    // TODO: For the list of activities, do we need to display the version at the
    //       time of activity here (specify head)?
    async fn latest(&self, ctx: &Context<'_>) -> Result<Option<MoleculeVersionedFileEntry>> {
        let read_versioned_file_entry_uc =
            from_catalog_n!(ctx, dyn MoleculeReadVersionedFileEntryUseCase);

        let maybe_versioned_file_entry = read_versioned_file_entry_uc
            .execute(&self.dataset_id, None, None)
            .await
            .map_err(|e| {
                use MoleculeReadVersionedFileEntryError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    E::Internal(e) => e.int_err().into(),
                }
            })?;

        Ok(maybe_versioned_file_entry
            .map(|entry| MoleculeVersionedFileEntry::new(self.dataset_id.clone(), entry)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeVersionedFileEntry {
    versioned_file_dataset_id: odf::DatasetID,
    entity: kamu_molecule_domain::MoleculeVersionedFileEntry,
}

impl MoleculeVersionedFileEntry {
    pub fn new(
        versioned_file_dataset_id: odf::DatasetID,
        entity: kamu_molecule_domain::MoleculeVersionedFileEntry,
    ) -> Self {
        Self {
            versioned_file_dataset_id,
            entity,
        }
    }
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl MoleculeVersionedFileEntry {
    async fn system_time(&self) -> DateTime<Utc> {
        self.entity.system_time
    }

    async fn event_time(&self) -> DateTime<Utc> {
        self.entity.event_time
    }

    async fn version(&self) -> u32 {
        self.entity.version
    }

    async fn content_hash(&self) -> Multihash<'_> {
        Multihash::from(&self.entity.content_hash)
    }

    async fn content_length(&self) -> usize {
        self.entity.content_length
    }

    async fn content_type(&self) -> &String {
        &self.entity.content_type.0
    }

    async fn access_level(&self) -> &MoleculeAccessLevel {
        &self.entity.basic_info.access_level
    }

    async fn change_by(&self) -> &String {
        &self.entity.basic_info.change_by
    }

    async fn description(&self) -> &Option<String> {
        &self.entity.basic_info.description
    }

    async fn categories(&self) -> &Vec<MoleculeCategory> {
        &self.entity.basic_info.categories
    }

    async fn tags(&self) -> &Vec<MoleculeTag> {
        &self.entity.basic_info.tags
    }

    async fn content_text(&self) -> Option<&str> {
        let detailed_info = &self.entity.detailed_info;
        detailed_info.content_text.as_deref()
    }

    async fn encryption_metadata(&self) -> Option<MoleculeEncryptionMetadata> {
        let detailed_info = &self.entity.detailed_info;
        detailed_info
            .encryption_metadata
            .as_ref()
            .map(|metadata_record| metadata_record.as_entity().into())
    }

    /// Returns encoded content in-band. Should be used for small files only and
    /// will return an error if called on large data.
    pub async fn content(&self, ctx: &Context<'_>) -> Result<Base64Usnp> {
        let molecule_versioned_file_content_provider =
            from_catalog_n!(ctx, dyn MoleculeVersionedFileContentProvider);

        let content_bytes = molecule_versioned_file_content_provider
            .get_versioned_file_content(&self.versioned_file_dataset_id, &self.entity.content_hash)
            .await
            .map_err(|e| {
                use MoleculeVersionedFileContentProviderError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(Base64Usnp::from(content_bytes))
    }

    /// Returns a direct download URL
    async fn content_url(&self, ctx: &Context<'_>) -> Result<VersionedFileContentDownload> {
        let molecule_versioned_file_content_provider =
            from_catalog_n!(ctx, dyn MoleculeVersionedFileContentProvider);

        let download_data = molecule_versioned_file_content_provider
            .get_versioned_file_content_download_data(
                &self.versioned_file_dataset_id,
                &self.entity.content_hash,
            )
            .await
            .map_err(|e| {
                use MoleculeVersionedFileContentProviderError as E;
                match e {
                    E::Access(e) => GqlError::Access(e),
                    e @ E::Internal(_) => e.int_err().into(),
                }
            })?;

        Ok(VersionedFileContentDownload::from(download_data))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MoleculeEncryptionMetadata {
    pub entity: kamu_molecule_domain::MoleculeEncryptionMetadata,
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
