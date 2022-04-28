// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::{MetadataBlock, Multihash};

use async_trait::async_trait;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait Dataset {
    async fn commit(&self, block: MetadataBlock) -> Result<CommitResult, CommitError>;

    fn as_metadata_chain(&self) -> &dyn MetadataChain2;
    fn as_data_repo(&self) -> &dyn ObjectRepository;
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitResult {
    new_head: Multihash,
    old_head: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

pub type InternalError = Box<dyn std::error::Error>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommitError {
    #[error("internal error")]
    Internal(InternalError),
}
