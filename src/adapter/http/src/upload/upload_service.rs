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
use serde::Serialize;

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait UploadService: Send + Sync {
    async fn organize_file_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct UploadContext {
    pub upload_url: String,
    pub method: &'static str,
    pub fields: Vec<UploadFormField>,
}

#[derive(Debug, Serialize)]
pub struct UploadFormField {
    pub name: &'static str,
    pub value: String,
}

///////////////////////////////////////////////////////////////////////////////
