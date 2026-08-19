// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait MoleculeVersionedFileContentProvider: Send + Sync {
    async fn get_versioned_file_content(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        content_hash: &odf::Multihash,
    ) -> Result<bytes::Bytes, MoleculeVersionedFileContentProviderError>;

    async fn get_versioned_file_content_download_data(
        &self,
        versioned_file_dataset_id: &odf::DatasetID,
        content_hash: &odf::Multihash,
    ) -> Result<odf::storage::GetExternalUrlResult, MoleculeVersionedFileContentProviderError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum MoleculeVersionedFileContentProviderError {
    #[error(transparent)]
    Access(#[from] odf::AccessError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
