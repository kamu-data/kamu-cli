// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_datasets::ReadCheckedDataset;
use kamu_molecule_domain::{
    MoleculeVersionedFileContentProvider,
    MoleculeVersionedFileContentProviderError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn MoleculeVersionedFileContentProvider)]
pub struct MoleculeVersionedFileContentProviderImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MoleculeVersionedFileContentProvider for MoleculeVersionedFileContentProviderImpl {
    async fn get_versioned_file_content(
        &self,
        versioned_file_dataset: &ReadCheckedDataset<'_>,
        content_hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, MoleculeVersionedFileContentProviderError> {
        let data_repo = versioned_file_dataset.as_data_repo();
        let file_bytes = data_repo.get_bytes(content_hash).await.int_err()?;
        Ok(file_bytes)
    }

    async fn get_versioned_file_content_download_data(
        &self,
        versioned_file_dataset: &ReadCheckedDataset<'_>,
        content_hash: &odf::Multihash,
    ) -> Result<odf::storage::GetExternalUrlResult, MoleculeVersionedFileContentProviderError> {
        let data_repo = versioned_file_dataset.as_data_repo();
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
