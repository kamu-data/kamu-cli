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

use crate::{DidEntity, DidSecretKey};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DidSecretKeyRepository: Send + Sync {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError>;

    async fn get_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<DidSecretKey, GetDidSecretKeyError>;

    async fn delete_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<(), DeleteDidSecretKeyError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SaveDidSecretKeyError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDidSecretKeyError {
    #[error(transparent)]
    NotFound(#[from] DidSecretKeyNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Entity's DID secret key not found: '{entity:?}'")]
pub struct DidSecretKeyNotFoundError {
    pub entity: DidEntity<'static>,
}

impl DidSecretKeyNotFoundError {
    pub fn new(entity: &DidEntity<'_>) -> Self {
        Self {
            entity: entity.clone().into_owned(),
        }
    }
}

impl From<DidEntity<'static>> for DidSecretKeyNotFoundError {
    fn from(entity: DidEntity<'static>) -> Self {
        Self { entity }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DeleteDidSecretKeyError {
    #[error(transparent)]
    NotFound(#[from] DidSecretKeyNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
