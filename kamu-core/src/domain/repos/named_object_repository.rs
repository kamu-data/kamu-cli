// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::InternalError;

use async_trait::async_trait;
use bytes::Bytes;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait NamedObjectRepository {
    /// Resolves reference to the object hash it's pointing to
    async fn get(&self, name: &str) -> Result<Bytes, GetError>;

    /// Update referece to point at the specified object hash
    async fn set(&self, name: &str, data: &[u8]) -> Result<(), InternalError>;

    /// Deletes specified reference
    async fn delete(&self, name: &str) -> Result<(), InternalError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("object does not exist: {name}")]
pub struct NotFoundError {
    pub name: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetError {
    #[error(transparent)]
    NotFound(NotFoundError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}
