// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::InternalError;
use opendatafabric::AccountID;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::io::AsyncRead;

use crate::AccessToken;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UploadService: Send + Sync {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
        content_length: i64,
        access_token: &AccessToken,
    ) -> Result<UploadContext, MakeUploadContextError>;

    async fn save_upload(
        &self,
        account_id: &AccountID,
        upload_id: String,
        file_name: String,
        file_data: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadContext {
    pub upload_url: String,
    pub method: String,
    pub headers: Vec<(String, String)>,
    pub fields: Vec<(String, String)>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum MakeUploadContextError {
    #[error(transparent)]
    TooLarge(ContentTooLargeError),
    #[error(transparent)]
    Internal(InternalError),
}

#[derive(Debug, Error)]
#[error("Content too large")]
pub struct ContentTooLargeError {}

///////////////////////////////////////////////////////////////////////////////
