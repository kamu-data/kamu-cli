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
use tokio::io::AsyncRead;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UploadService: Send + Sync {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError>;

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
    // TODO: resulting URL
    pub fields: Vec<UploadFormField>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UploadFormField {
    pub name: String,
    pub value: String,
}

///////////////////////////////////////////////////////////////////////////////
