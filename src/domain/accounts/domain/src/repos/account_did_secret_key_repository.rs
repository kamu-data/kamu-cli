// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::DidSecretKey;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccountDidSecretKeyRepository: Send + Sync {
    async fn save_did_secret_key(
        &self,
        account_id: &odf::AccountID,
        owner_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveAccountDidSecretKeyError>;

    async fn get_did_secret_keys_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByAccountIdError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveAccountDidSecretKeyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum GetDidSecretKeysByAccountIdError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
