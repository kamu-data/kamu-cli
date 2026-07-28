// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_datasets::{DatasetAction, ReadCheckedDataset};
use kamu_molecule_domain::{
    MoleculeVersionedFileContentProvider,
    MoleculeVersionedFileContentProviderError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeVersionedFileContentProvider)]
pub struct MoleculeVersionedFileContentProviderImpl {
    rebac_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MoleculeVersionedFileContentProviderImpl {
    async fn readable_versioned_file_dataset(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
    ) -> Result<ReadCheckedDataset<'static>, MoleculeVersionedFileContentProviderError> {
        let readable_dataset = self
            .rebac_registry_facade
            .resolve_dataset_by_ref(
                &versioned_file_dataset_id.as_local_ref(),
                DatasetAction::Read,
            )
            .await
            .map_err(|e| match e {
                RebacDatasetRefUnresolvedError::NotFound(e) => e.int_err().into(),
                RebacDatasetRefUnresolvedError::Access(e) => {
                    MoleculeVersionedFileContentProviderError::Access(e)
                }
                e @ RebacDatasetRefUnresolvedError::Internal(_) => e.int_err().into(),
            })?;

        Ok(ReadCheckedDataset::from_owned(readable_dataset))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MoleculeVersionedFileContentProvider for MoleculeVersionedFileContentProviderImpl {
    async fn get_versioned_file_content(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        content_hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, MoleculeVersionedFileContentProviderError> {
        let readable_versioned_file_dataset = self
            .readable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let data_repo = readable_versioned_file_dataset.as_data_repo();
        let file_bytes = data_repo.get_bytes(content_hash).await.int_err()?;
        Ok(file_bytes)
    }

    async fn get_versioned_file_content_download_data(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        content_hash: &odf::Multihash,
    ) -> Result<odf::storage::GetExternalUrlResult, MoleculeVersionedFileContentProviderError> {
        let readable_versioned_file_dataset = self
            .readable_versioned_file_dataset(versioned_file_dataset_id)
            .await?;

        let data_repo = readable_versioned_file_dataset.as_data_repo();
        let download = match data_repo
            .get_external_download_url(content_hash, odf::storage::ExternalTransferOpts::default())
            .await
        {
            Ok(res) => res,
            Err(err @ odf::storage::GetExternalUrlError::NotSupported) => {
                return Err(err.int_err().into());
            }
            Err(err) => return Err(err.int_err().into()),
        };
        Ok(download)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
