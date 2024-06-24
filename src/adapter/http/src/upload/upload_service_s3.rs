// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::ObjectCannedAcl;
use bytes::Bytes;
use dill::*;
use kamu::domain::{ErrorIntoInternal, InternalError};
use kamu::utils::s3_context::S3Context;
use opendatafabric::AccountID;
use tokio::io::AsyncRead;
use uuid::Uuid;

use crate::{
    make_upload_token,
    ContentTooLargeError,
    FileUploadLimitConfig,
    MakeUploadContextError,
    SaveUploadError,
    UploadContext,
    UploadNotSupportedError,
    UploadService,
};

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UploadService)]
pub struct UploadServiceS3 {
    s3_upload_context: S3Context,
    upload_config: Arc<FileUploadLimitConfig>,
}

impl UploadServiceS3 {
    pub fn new(s3_upload_context: S3Context, upload_config: Arc<FileUploadLimitConfig>) -> Self {
        Self {
            s3_upload_context,
            upload_config,
        }
    }

    fn make_file_key(&self, account_id: &AccountID, upload_id: &str, file_name: &str) -> String {
        format!("{}/{}/{}", account_id.as_multibase(), upload_id, file_name,)
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceS3 {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
        content_type: String,
        content_length: usize,
    ) -> Result<UploadContext, MakeUploadContextError> {
        if content_length > self.upload_config.max_file_size_in_bytes() {
            return Err(MakeUploadContextError::TooLarge(ContentTooLargeError {}));
        }

        let upload_id = Uuid::new_v4().simple().to_string();
        let file_key = self.make_file_key(account_id, &upload_id, &file_name);

        let presigned_conf = PresigningConfig::builder()
            .expires_in(
                chrono::Duration::try_seconds(3600)
                    .unwrap()
                    .to_std()
                    .unwrap(),
            )
            .build()
            .expect("Invalid presigning config");

        let presigned_request = self
            .s3_upload_context
            .client
            .put_object()
            .acl(ObjectCannedAcl::Private)
            .bucket(&self.s3_upload_context.bucket)
            .key(file_key.clone())
            .presigned(presigned_conf)
            .await
            .map_err(|e| MakeUploadContextError::Internal(e.int_err()))?;

        let upload_token = make_upload_token(upload_id, file_name, content_type, content_length);

        Ok(UploadContext {
            upload_url: String::from(presigned_request.uri()),
            upload_token,
            method: String::from("PUT"),
            use_multipart: false, // Only POST Policy would require multiparts
            headers: presigned_request
                .headers()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            fields: vec![],
        })
    }

    async fn upload_reference_size(
        &self,
        account_id: &AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<usize, InternalError> {
        let file_key = self.make_file_key(account_id, upload_id, file_name);

        let res = self
            .s3_upload_context
            .head_object(file_key)
            .await
            .map_err(ErrorIntoInternal::int_err)?;
        Ok(usize::try_from(res.content_length).unwrap())
    }

    async fn upload_reference_into_stream(
        &self,
        account_id: &AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, InternalError> {
        let file_key = self.make_file_key(account_id, upload_id, file_name);
        let resp = self
            .s3_upload_context
            .get_object(file_key)
            .await
            .map_err(ErrorIntoInternal::int_err)?;
        let stream = resp.body.into_async_read();
        Ok(Box::new(stream))
    }

    async fn save_upload(
        &self,
        _: &AccountID,
        _: &str,
        _: usize,
        _: Bytes,
    ) -> Result<(), SaveUploadError> {
        Err(SaveUploadError::NotSupported(UploadNotSupportedError {}))
    }
}

///////////////////////////////////////////////////////////////////////////////
