// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use aws_credential_types::Credentials;
use aws_sigv4::sign::v4::{calculate_signature, generate_signing_key};
use base64::prelude::*;
use chrono::{DateTime, Utc};
use dill::*;
use kamu::domain::{ErrorIntoInternal, InternalError};
use kamu::utils::s3_context::S3Context;
use opendatafabric::AccountID;
use thiserror::Error;
use tokio::io::AsyncRead;
use url::Url;
use uuid::Uuid;

use crate::{UploadContext, UploadFormField, UploadService};

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

    fn generate_amz_fields(
        &self,
        credentials: &Credentials,
        request_time: DateTime<Utc>,
    ) -> AmzFields {
        let request_date = format!("{}", request_time.format("%Y%m%d"));

        AmzFields {
            x_amz_algorithm: "AWS4-HMAC-SHA256",
            x_amz_date: request_time.to_rfc3339(),
            x_amz_credential: format!(
                "{}/{}/{}/s3/aws4_request",
                credentials.access_key_id(),
                request_date,
                self.s3_upload_context.region().unwrap()
            ),
        }
    }

    fn generate_post_policy(
        &self,
        key: &str,
        request_time: DateTime<Utc>,
        amz_fields: &AmzFields,
    ) -> String {
        let valid_util = request_time + Duration::from_secs(60);

        let policy_json = serde_json::json!({
            "expiration": valid_util.to_rfc3339(),
            "conditions": [
                { "acl": "private" },
                { "bucket": self.upload_s3_bucket_config.bucket_name },
                [ "content-length-range", 0, self.upload_s3_bucket_config.max_file_size_mb * 1024 * 1024 ],
                { "key": key },

                { "x-amz-algorithm": amz_fields.x_amz_algorithm },
                { "x-amz-credential": amz_fields.x_amz_credential.clone() },
                { "x-amz-date": amz_fields.x_amz_date.clone() },

            ]
        });
        policy_json.to_string()
    }

    fn sign_policy(
        &self,
        post_policy_base64: &str,
        credentials: &Credentials,
        request_time: SystemTime,
    ) -> String {
        let signing_key = generate_signing_key(
            credentials.secret_access_key(),
            request_time,
            self.s3_upload_context.region().unwrap(),
            "s3",
        );

        return calculate_signature(signing_key, post_policy_base64.as_bytes());
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceS3 {
    async fn make_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError> {
        let credentials = self.s3_upload_context.credentials().await;

        let upload_id = Uuid::new_v4().simple().to_string();
        let file_key = format!("{}/{}/{}", account_id.as_multibase(), upload_id, file_name,);

        let request_time = SystemTime::now();
        let request_chrono_time: DateTime<Utc> = request_time.into();

        let amz_fields = self.generate_amz_fields(&credentials, request_chrono_time);
        let post_policy = self.generate_post_policy(&file_key, request_chrono_time, &amz_fields);
        let post_policy_base64 = BASE64_STANDARD.encode(post_policy.as_bytes());
        let signature = self.sign_policy(&post_policy_base64, &credentials, request_time);

        Ok(UploadContext {
            upload_url: self.upload_s3_bucket_config.bucket_http_url.to_string(),
            method: "POST".to_string(),
            fields: vec![
                UploadFormField {
                    name: "key".to_string(),
                    value: file_key,
                },
                UploadFormField {
                    name: "acl".to_string(),
                    value: String::from("private"),
                },
                UploadFormField {
                    name: "x-amz-server-side-encryption".to_string(),
                    value: String::from("AES256"),
                },
                UploadFormField {
                    name: "x-amz-credential".to_string(),
                    value: amz_fields.x_amz_credential,
                },
                UploadFormField {
                    name: "x-amz-date".to_string(),
                    value: amz_fields.x_amz_date,
                },
                UploadFormField {
                    name: "x-amz-algorithm".to_string(),
                    value: String::from(amz_fields.x_amz_algorithm),
                },
                UploadFormField {
                    name: "policy".to_string(),
                    value: post_policy_base64,
                },
                UploadFormField {
                    name: "signature".to_string(),
                    value: signature,
                },
            ],
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
    pub max_file_size_mb: i32,
}

///////////////////////////////////////////////////////////////////////////////

struct AmzFields {
    x_amz_date: String,
    x_amz_credential: String,
    x_amz_algorithm: &'static str,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Direct file uploads are not supported in this environment")]
struct UploadNotSupportedError {}

///////////////////////////////////////////////////////////////////////////////
