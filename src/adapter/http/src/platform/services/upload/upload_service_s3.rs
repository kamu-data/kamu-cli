// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::types::ObjectCannedAcl;
use bytes::Bytes;
use dill::*;
use internal_error::ErrorIntoInternal;
use kamu_core::services::upload_service::*;
use kamu_core::MediaType;
use s3_utils::{PutObjectOptions, S3Context};
use tokio::io::AsyncRead;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn UploadService)]
pub struct UploadServiceS3 {
    upload_config: Arc<FileUploadLimitConfig>,

    #[component(explicit)]
    s3_upload_context: S3Context,
}

impl UploadServiceS3 {
    fn make_file_key(
        &self,
        account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> String {
        format!("{}/{}/{}", account_id.as_multibase(), upload_id, file_name,)
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceS3 {
    async fn make_upload_context(
        &self,
        owner_account_id: &odf::AccountID,
        file_name: String,
        content_type: Option<MediaType>,
        content_length: usize,
    ) -> Result<UploadContext, MakeUploadContextError> {
        if content_length > self.upload_config.max_file_size_in_bytes() {
            return Err(MakeUploadContextError::TooLarge(ContentTooLargeError {}));
        }

        let upload_id = Uuid::new_v4().simple().to_string();
        let file_key = self.make_file_key(owner_account_id, &upload_id, &file_name);

        let presigned_config = PresigningConfig::builder()
            .expires_in(chrono::Duration::seconds(3600).to_std().unwrap())
            .build()
            .expect("Invalid presigning config");

        let presigned_request = self
            .s3_upload_context
            .put_object_presigned_request(
                file_key,
                PutObjectOptions::builder()
                    .acl(ObjectCannedAcl::Private)
                    .presigned_config(presigned_config)
                    .build(),
            )
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        let owner_account_id_mb = owner_account_id.as_multibase().to_stack_string();

        let upload_token = UploadTokenBase64Json(UploadToken {
            upload_id,
            file_name,
            owner_account_id: owner_account_id_mb.to_string(),
            content_length,
            content_type,
        });

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
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<usize, UploadTokenIntoStreamError> {
        let file_key = self.make_file_key(owner_account_id, upload_id, file_name);

        let res = self
            .s3_upload_context
            .head_object(file_key)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "S3 head object failed");
                if let SdkError::ServiceError(_) = e {
                    UploadTokenIntoStreamError::ContentNotFound(ContentNotFoundError {})
                } else {
                    UploadTokenIntoStreamError::Internal(e.int_err())
                }
            })?;

        let content_length = res
            .content_length
            .ok_or_else(|| "S3 did not return content length".int_err())?;

        Ok(usize::try_from(content_length).unwrap())
    }

    async fn upload_reference_into_stream(
        &self,
        owner_account_id: &odf::AccountID,
        upload_id: &str,
        file_name: &str,
    ) -> Result<Box<dyn AsyncRead + Send + Unpin>, UploadTokenIntoStreamError> {
        let file_key = self.make_file_key(owner_account_id, upload_id, file_name);
        let resp = self
            .s3_upload_context
            .get_object(file_key)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "S3 get object failed");
                if let SdkError::ServiceError(_) = e {
                    UploadTokenIntoStreamError::ContentNotFound(ContentNotFoundError {})
                } else {
                    UploadTokenIntoStreamError::Internal(e.int_err())
                }
            })?;

        let stream = resp.body.into_async_read();
        Ok(Box::new(stream))
    }

    async fn save_upload(
        &self,
        _: &UploadToken,
        _: usize,
        _: Bytes,
    ) -> Result<(), SaveUploadError> {
        Err(SaveUploadError::NotSupported(UploadNotSupportedError {}))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
