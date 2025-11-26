// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use file_utils::MediaType;
use internal_error::InternalError;
use odf::dataset::RefCASError;
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::{ExtraDataFields, FileVersion, WriteCheckedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UpdateVersionFileUseCase: Send + Sync {
    async fn execute(
        &self,
        file_dataset: WriteCheckedDataset<'_>,
        content_args_maybe: Option<ContentArgs>,
        expected_head: Option<odf::Multihash>,
        extra_data: Option<ExtraDataFields>,
    ) -> Result<UpdateVersionFileResult, UpdateVersionFileUseCaseError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ContentArgs {
    pub content_length: usize,
    pub content_hash: odf::Multihash,
    pub content_stream: Option<Box<dyn AsyncRead + Send + Unpin>>,
    pub content_type: Option<MediaType>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UpdateVersionFileResult {
    pub new_version: FileVersion,
    pub old_head: odf::Multihash,
    pub new_head: odf::Multihash,
    pub content_hash: odf::Multihash,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateVersionFileUseCaseError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    RefCASFailed(#[from] RefCASError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
