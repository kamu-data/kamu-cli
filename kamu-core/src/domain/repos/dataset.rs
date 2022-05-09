// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use opendatafabric::Multihash;

use async_trait::async_trait;
use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait Dataset: Send + Sync {
    /// Returns a brief summary of the dataset
    async fn get_summary(&self, opts: SummaryOptions) -> Result<DatasetSummary, GetSummaryError>;

    fn as_metadata_chain(&self) -> &dyn MetadataChain2;
    fn as_data_repo(&self) -> &dyn ObjectRepository;
    fn as_checkpoint_repo(&self) -> &dyn ObjectRepository;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct SummaryOptions {
    pub update_if_stale: bool,
}

impl Default for SummaryOptions {
    fn default() -> Self {
        Self {
            update_if_stale: true,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct CommitResult {
    pub new_head: Multihash,
    pub old_head: Multihash,
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Expected object of type {expected} but got {actual}")]
pub struct InvalidObjectKind {
    pub expected: String,
    pub actual: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetSummaryError {
    #[error("Dataset is empty")]
    EmptyDataset,
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CommitError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}
