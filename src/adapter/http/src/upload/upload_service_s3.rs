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
use dill::*;
use kamu::domain::{ErrorIntoInternal, InternalError};
use kamu::utils::s3_context::S3Context;
use opendatafabric::AccountID;
use thiserror::Error;
use tokio::io::AsyncRead;
use url::Url;
use uuid::Uuid;

use crate::{
    AccessToken,
    ContentTooLargeError,
    MakeUploadContextError,
    UploadContext,
    UploadService,
};

///////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn UploadService)]
pub struct UploadServiceS3 {
    s3_upload_context: S3Context,
    upload_s3_bucket_config: Arc<UploadS3BucketConfig>,
}

impl UploadServiceS3 {
    pub fn new(
        s3_upload_context: S3Context,
        upload_s3_bucket_config: Arc<UploadS3BucketConfig>,
    ) -> Self {
        Self {
            s3_upload_context,
            upload_s3_bucket_config,
        }
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceS3 {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
        content_length: i64,
        _: &AccessToken, // S3 does not require our own token
    ) -> Result<UploadContext, MakeUploadContextError> {
        if content_length > self.upload_s3_bucket_config.max_file_size_bytes {
            return Err(MakeUploadContextError::TooLarge(ContentTooLargeError {}));
        }

        let upload_id = Uuid::new_v4().simple().to_string();
        let file_key = format!("{}/{}/{}", account_id.as_multibase(), upload_id, file_name,);

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

        Ok(UploadContext {
            upload_url: String::from(presigned_request.uri()),
            method: String::from("PUT"),
            headers: presigned_request
                .headers()
                .map(|(key, value)| (key.to_string(), value.to_string()))
                .collect(),
            fields: vec![],
        })
    }

    async fn save_upload(
        &self,
        _: &AccountID,
        _: String,
        _: String,
        _: Box<dyn AsyncRead + Send + Unpin>,
    ) -> Result<(), InternalError> {
        let err = UploadNotSupportedError {};
        Err(err.int_err())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UploadS3BucketConfig {
    pub bucket_http_url: Url,
    pub bucket_name: String,
    pub max_file_size_bytes: i64,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Direct file uploads are not supported in this environment")]
struct UploadNotSupportedError {}

///////////////////////////////////////////////////////////////////////////////
