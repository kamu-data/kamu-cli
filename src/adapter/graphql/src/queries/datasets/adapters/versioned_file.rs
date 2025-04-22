// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::DatasetRequestStateWithOwner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) type FileVersion = u32;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct VersionedFile {
    state: DatasetRequestStateWithOwner,
}

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl VersionedFile {
    #[graphql(skip)]
    pub fn new(state: DatasetRequestStateWithOwner) -> Self {
        Self { state }
    }

    /// Returns encoded content in-band. Can be used for very small files only.
    #[tracing::instrument(level = "info", name = VersionedFile_get_content, skip_all)]
    pub async fn get_content(
        &self,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> GetFileContentResult {
        todo!()
    }

    /// Returns a direct download URL for use with large files
    #[tracing::instrument(level = "info", name = VersionedFile_get_content_url, skip_all)]
    pub async fn get_content_url(
        &self,
        as_of_version: Option<FileVersion>,
        as_of_block_hash: Option<Multihash<'static>>,
    ) -> GetFileContentUrlResult {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum GetFileContentResult {
    Success(GetFileContentSuccess),
    NotFound(GetFileContentErrorNotFound),
    TooLarge(GetFileContentErrorTooLarge),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentSuccess {
    /// File version this result corresponds to
    pub version: FileVersion,

    /// Block that this result corresponds to
    pub block_hash: Multihash<'static>,

    /// Base64-encoded data (url-safe, no padding)
    pub content: Base64Usnp,

    /// Media type of the file content
    pub content_type: String,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,
}
#[ComplexObject]
impl GetFileContentSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

pub struct GetFileContentErrorNotFound;
#[Object]
impl GetFileContentErrorNotFound {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> &str {
        "Specified version or block not found"
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentErrorTooLarge {
    in_band_content_size_limit: usize,
}
#[ComplexObject]
impl GetFileContentErrorTooLarge {
    async fn is_success(&self) -> bool {
        false
    }
    async fn error_message(&self) -> String {
        format!(
            "This file is too large to return in-band (>{}). Use getContentUrl instead",
            humansize::format_size(self.in_band_content_size_limit, humansize::BINARY)
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(
    field(name = "is_success", ty = "bool"),
    field(name = "error_message", ty = "String")
)]
pub enum GetFileContentUrlResult {
    Success(GetFileContentUrlSuccess),
    NotFound(GetFileContentErrorNotFound),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct GetFileContentUrlSuccess {
    /// File version this result corresponds to
    pub version: FileVersion,

    /// Block that this result corresponds to
    pub block_hash: Multihash<'static>,

    /// Direct download URL
    pub content_url: String,

    /// Media type of the file content
    pub content_type: String,

    /// Extra data associated with this file version
    pub extra_data: serde_json::Value,
}
#[ComplexObject]
impl GetFileContentUrlSuccess {
    async fn is_success(&self) -> bool {
        true
    }
    async fn error_message(&self) -> String {
        String::new()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
