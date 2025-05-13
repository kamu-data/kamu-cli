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

use crate::{DidEntity, DidEntityType, DidSecretKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DidSecretKeyRepository: Send + Sync {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError>;

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
        entity_type: Option<DidEntityType>,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByCreatorIdError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDidSecretKeyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum GetDidSecretKeysByCreatorIdError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
