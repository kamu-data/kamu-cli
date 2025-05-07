// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crypto_utils::DidSecretKey;
use internal_error::InternalError;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetDidSecretKeyRepository: Send + Sync {
    async fn save_did_secret_key(
        &self,
        dataset_id: &odf::DatasetID,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDatasetDidSecretKeyError>;

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
    ) -> Result<Vec<DidSecretKey>, GetDatasetDidSecretKeysByOwnerIdError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDatasetDidSecretKeyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum GetDatasetDidSecretKeysByOwnerIdError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, Clone, sqlx::FromRow, PartialEq, Eq)]
pub struct DatasetDidSecretKeyRowModel {
    pub dataset_id: odf::DatasetID,
    pub creator_id: odf::AccountID,
    pub secret_key: Vec<u8>,
    pub secret_nonce: Vec<u8>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
impl From<DatasetDidSecretKeyRowModel> for DidSecretKey {
    fn from(value: DatasetDidSecretKeyRowModel) -> Self {
        DidSecretKey {
            secret_key: value.secret_key,
            secret_nonce: value.secret_nonce,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
