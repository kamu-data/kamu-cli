// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use aws_credential_types::Credentials;
use base64::prelude::*;
use chrono::{DateTime, Duration, Utc};
use dill::*;
use kamu::domain::InternalError;
use kamu::utils::s3_context::S3Context;
use opendatafabric::AccountID;
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
        let valid_util = request_time + Duration::minutes(1);

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
        _post_policy_base64: &str,
        _credentials: &Credentials,
        _request_time: DateTime<Utc>,
        _amz_fields: &AmzFields,
    ) -> String {
        // TODO
        String::from("<todo:signature>")
    }
}

#[async_trait::async_trait]
impl UploadService for UploadServiceS3 {
    async fn organize_file_upload_context(
        &self,
        account_id: &AccountID,
        file_name: String,
    ) -> Result<UploadContext, InternalError> {
        let credentials = self.s3_upload_context.credentials().await;

        let upload_id = Uuid::new_v4().simple().to_string();
        let file_key = format!("{}/{}/{}", account_id.as_multibase(), upload_id, file_name,);

        let request_time = Utc::now();
        let amz_fields = self.generate_amz_fields(&credentials, request_time);
        let post_policy = self.generate_post_policy(&file_key, request_time, &amz_fields);

        let post_policy_base64 = BASE64_STANDARD.encode(post_policy.as_bytes());
        let signature =
            self.sign_policy(&post_policy_base64, &credentials, request_time, &amz_fields);

        Ok(UploadContext {
            upload_url: self.upload_s3_bucket_config.bucket_http_url.to_string(),
            method: "POST",
            fields: vec![
                UploadFormField {
                    name: "key",
                    value: file_key,
                },
                UploadFormField {
                    name: "acl",
                    value: String::from("private"),
                },
                UploadFormField {
                    name: "x-amz-server-side-encryption",
                    value: String::from("AES256"),
                },
                UploadFormField {
                    name: "x-amz-credential",
                    value: amz_fields.x_amz_credential,
                },
                UploadFormField {
                    name: "x-amz-date",
                    value: amz_fields.x_amz_date,
                },
                UploadFormField {
                    name: "x-amz-algorithm",
                    value: String::from(amz_fields.x_amz_algorithm),
                },
                UploadFormField {
                    name: "policy",
                    value: post_policy_base64,
                },
                UploadFormField {
                    name: "signature",
                    value: signature,
                },
            ],
        })
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
