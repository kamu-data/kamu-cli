// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::sync::Arc;

use async_utils::AsyncReadObj;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::{CreateBucketError, CreateBucketOutput};
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::presigning::{PresignedRequest, PresigningConfig};
use aws_sdk_s3::types::{CommonPrefix, Delete, ObjectCannedAcl, ObjectIdentifier};
use internal_error::{InternalError, ResultIntoInternal, *};
use url::Url;

use crate::S3Metrics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct S3ContextSharedState {
    endpoint: Option<String>,
    bucket: String,
    bucket_url: String,
}

impl S3ContextSharedState {
    fn new(endpoint: Option<String>, bucket: String) -> Self {
        let bucket_url = match &endpoint {
            Some(endpoint) => {
                format!("s3+{endpoint}/{bucket}")
            }
            None => {
                format!("s3://{bucket}")
            }
        };

        Self {
            endpoint,
            bucket,
            bucket_url,
        }
    }
}

struct S3ContextState {
    key_prefix: String,
    url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    client: Client,
    shared_state: Arc<S3ContextSharedState>,
    state: Arc<S3ContextState>,
    // Out of state, since it can change
    maybe_metrics: Option<Arc<S3Metrics>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
impl S3Context {
    const MAX_LISTED_OBJECTS: i32 = 1000;

    pub fn builder() -> S3ContextBuilder {
        S3ContextBuilder::new()
    }

    fn make_url(endpoint: Option<&str>, bucket: &str, key_prefix: &str) -> Url {
        let context_url_str = match endpoint {
            Some(endpoint) => {
                format!("s3+{endpoint}/{bucket}/{key_prefix}")
            }
            None => {
                format!("s3://{bucket}/{key_prefix}")
            }
        };
        Url::parse(context_url_str.as_str()).unwrap()
    }

    #[inline]
    pub fn endpoint(&self) -> Option<&str> {
        self.shared_state.endpoint.as_deref()
    }

    #[inline]
    pub fn bucket(&self) -> &str {
        &self.shared_state.bucket
    }

    #[inline]
    pub fn key_prefix(&self) -> &str {
        &self.state.key_prefix
    }

    #[inline]
    pub fn url(&self) -> &Url {
        &self.state.url
    }

    fn new(
        client: Client,
        endpoint: Option<impl Into<String>>,
        bucket: impl Into<String>,
        key_prefix: impl Into<String>,
        maybe_metrics: Option<Arc<S3Metrics>>,
    ) -> Self {
        let endpoint = endpoint.map(Into::into);
        let bucket = bucket.into();
        let key_prefix = key_prefix.into();

        let url = Self::make_url(endpoint.as_deref(), &bucket, &key_prefix);

        Self {
            client,
            shared_state: Arc::new(S3ContextSharedState::new(endpoint, bucket)),
            state: Arc::new(S3ContextState { key_prefix, url }),
            maybe_metrics,
        }
    }

    /// Creates a context for a sub-key while reusing the S3 client and its
    /// credential cache
    pub fn sub_context(&self, sub_key: &str) -> Self {
        self.clone().into_sub_context(sub_key)
    }

    /// Moves context under a sub-key
    pub fn into_sub_context(mut self, sub_key: &str) -> Self {
        let mut key_prefix = self.get_key(sub_key);
        if !key_prefix.ends_with('/') {
            key_prefix.push('/');
        }

        self.state = Arc::new(S3ContextState {
            url: Self::make_url(
                self.shared_state.endpoint.as_deref(),
                &self.shared_state.bucket,
                &key_prefix,
            ),
            key_prefix,
        });
        self
    }

    pub fn region(&self) -> Option<&str> {
        self.client.config().region().map(AsRef::as_ref)
    }

    pub fn get_key(&self, sub_key: &str) -> String {
        if self.state.key_prefix.is_empty() {
            sub_key.to_string()
        } else {
            format!("{}{}", self.state.key_prefix, sub_key)
        }
    }

    pub async fn api_call<F, FFut, R, E>(&self, sdk_method: &str, f: F) -> Result<R, E>
    where
        F: FnOnce() -> FFut,
        FFut: std::future::Future<Output = Result<R, E>>,
    {
        if let Some(metrics) = &self.maybe_metrics {
            // A call with metrics fixation
            let start = std::time::Instant::now();
            let api_call_result = f().await;
            let elapsed = start.elapsed();

            let metric_labels = [self.shared_state.bucket_url.as_str(), sdk_method];

            metrics
                .s3_api_request_time_s_hist
                .with_label_values(&metric_labels)
                .observe(elapsed.as_secs_f64());

            let counter_metric = if api_call_result.is_ok() {
                &metrics.s3_api_call_count_successful_num_total
            } else {
                &metrics.s3_api_call_count_failed_num_total
            };
            counter_metric.with_label_values(&metric_labels).inc();

            api_call_result
        } else {
            // Just a call without metrics
            f().await
        }
    }

    pub async fn create_bucket(
        &self,
        bucket: &str,
    ) -> Result<CreateBucketOutput, SdkError<CreateBucketError>> {
        self.client.create_bucket().bucket(bucket).send().await
    }

    pub async fn head_object(
        &self,
        key: String,
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
        self.api_call("head_object", || async {
            self.client
                .head_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .send()
                .await
        })
        .await
    }

    pub async fn get_object(
        &self,
        key: String,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
        self.api_call("get_object", || async {
            self.client
                .get_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .send()
                .await
        })
        .await
    }

    pub async fn get_object_presigned_request(
        &self,
        key: impl Into<String>,
        options: GetObjectOptions,
    ) -> Result<PresignedRequest, SdkError<GetObjectError>> {
        self.api_call("get_object(presigned)", || async {
            self.client
                .get_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .presigned(options.presigned_config)
                .await
        })
        .await
    }

    pub async fn put_object(
        &self,
        key: String,
        data: &[u8],
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        self.api_call("put_object", || async {
            let size = i64::try_from(data.len()).unwrap();

            self.client
                .put_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                // TODO: PERF: Avoid copying data into a buffer
                .body(Vec::from(data).into())
                .content_length(size)
                .send()
                .await
        })
        .await
    }

    pub async fn put_object_presigned_request(
        &self,
        key: impl Into<String>,
        options: PutObjectOptions,
    ) -> Result<PresignedRequest, SdkError<PutObjectError>> {
        self.api_call("put_object(presigned)", || async {
            self.client
                .put_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .set_acl(options.acl)
                .presigned(options.presigned_config)
                .await
        })
        .await
    }

    pub async fn put_object_stream(
        &self,
        key: String,
        reader: Box<AsyncReadObj>,
        size: u64,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        self.api_call("put_object(stream)", || async {
            use aws_smithy_types::body::SdkBody;
            use aws_smithy_types::byte_stream::ByteStream;

            // FIXME: https://github.com/awslabs/aws-sdk-rust/issues/1030
            let stream = tokio_util::io::ReaderStream::new(reader);
            let body = reqwest::Body::wrap_stream(stream);
            let byte_stream = ByteStream::new(SdkBody::from_body_1_x(body));

            self.client
                .put_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .body(byte_stream)
                .content_length(i64::try_from(size).unwrap())
                .send()
                .await
        })
        .await
    }

    pub async fn delete_object(
        &self,
        key: String,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
        self.api_call("delete_object", || async {
            self.client
                .delete_object()
                .bucket(self.shared_state.bucket.clone())
                .key(key)
                .send()
                .await
        })
        .await
    }

    pub async fn bucket_path_exists(&self, key_prefix: &str) -> Result<bool, InternalError> {
        self.api_call("list_objects_v2(bucket_path_exists)", || async {
            let listing = self
                .client
                .list_objects_v2()
                .bucket(self.shared_state.bucket.clone())
                .prefix(self.get_key(key_prefix))
                .max_keys(1)
                .send()
                .await;

            match listing {
                Ok(resp) => Ok(resp.contents.is_some()),
                Err(e) => Err(e.int_err()),
            }
        })
        .await
    }

    pub async fn bucket_list_folders(&self) -> Result<Vec<CommonPrefix>, InternalError> {
        self.api_call("list_objects_v2(bucket_list_folders)", || async {
            let mut prefixes = Vec::new();

            let mut paginator = self
                .client
                .list_objects_v2()
                .bucket(self.shared_state.bucket.clone())
                .delimiter("/")
                .max_keys(Self::MAX_LISTED_OBJECTS)
                .into_paginator()
                .send();

            while let Some(page) = paginator.next().await {
                let mut page_prefixes = page.int_err()?.common_prefixes.unwrap_or_default();
                prefixes.append(&mut page_prefixes);
            }

            Ok(prefixes)
        })
        .await
    }

    pub async fn recursive_delete(&self, key_prefix: String) -> Result<(), InternalError> {
        // ListObjectsV2Request returns at most S3Context::MAX_LISTED_OBJECTS=1000 items
        let mut has_next_page = true;
        while has_next_page {
            let list_response = self
                .api_call("list_objects_v2(recursive_delete)", || async {
                    self.client
                        .list_objects_v2()
                        .bucket(self.shared_state.bucket.clone())
                        .prefix(&key_prefix)
                        .max_keys(Self::MAX_LISTED_OBJECTS)
                        .send()
                        .await
                })
                .await
                .int_err()?;

            if let Some(contents) = list_response.contents {
                has_next_page = list_response.is_truncated.unwrap_or_default();

                let object_identifiers = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect::<Result<Vec<_>, _>>()
                    .int_err()?;
                let delete_request = Delete::builder()
                    .set_objects(Some(object_identifiers))
                    .quiet(true)
                    .build()
                    .int_err()?;

                self.api_call("delete_objects(recursive_delete)", || async {
                    self.client
                        .delete_objects()
                        .bucket(self.shared_state.bucket.clone())
                        .delete(delete_request)
                        .send()
                        .await
                })
                .await
                .int_err()?;
            } else {
                has_next_page = false;
            }
        }

        Ok(())
    }

    pub async fn recursive_move(
        &self,
        old_key_prefix: String,
        new_key_prefix: String,
    ) -> Result<(), InternalError> {
        // ListObjectsV2Request returns at most S3Context::MAX_LISTED_OBJECTS=1000 items
        let mut has_next_page = true;
        while has_next_page {
            let list_response = self
                .api_call("list_objects_v2(recursive_move)", || async {
                    self.client
                        .list_objects_v2()
                        .bucket(self.shared_state.bucket.clone())
                        .prefix(&old_key_prefix)
                        .max_keys(Self::MAX_LISTED_OBJECTS)
                        .send()
                        .await
                })
                .await
                .int_err()?;

            // TODO: concurrency safety.
            // It is important not to allow parallel writes of Head reference file in the
            // same bucket. Consider optimistic locking (comparing old head with
            // expected before final commit).

            has_next_page = list_response.is_truncated.unwrap_or_default();
            if let Some(contents) = list_response.contents {
                for obj in &contents {
                    let copy_source =
                        format!("{}/{}", self.shared_state.bucket, obj.key.as_ref().unwrap());
                    let new_key = obj
                        .key
                        .as_ref()
                        .unwrap()
                        .replace(old_key_prefix.as_str(), new_key_prefix.as_str());

                    self.api_call("copy_object(recursive_move)", || async {
                        self.client
                            .copy_object()
                            .bucket(self.shared_state.bucket.clone())
                            .copy_source(copy_source)
                            .key(new_key)
                            .send()
                            .await
                    })
                    .await
                    .int_err()?;
                }

                let object_identifiers = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect::<Result<Vec<_>, _>>()
                    .int_err()?;
                let delete_request = Delete::builder()
                    .set_objects(Some(object_identifiers))
                    .quiet(true)
                    .build()
                    .int_err()?;

                self.api_call("delete_objects(recursive_move)", || async {
                    self.client
                        .delete_objects()
                        .bucket(self.shared_state.bucket.clone())
                        .delete(delete_request)
                        .send()
                        .await
                })
                .await
                .int_err()?;
            } else {
                has_next_page = false;
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder)]
pub struct GetObjectOptions {
    presigned_config: PresigningConfig,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(bon::Builder)]
pub struct PutObjectOptions {
    presigned_config: PresigningConfig,
    acl: Option<ObjectCannedAcl>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct S3ContextBuilder {
    region: Option<String>,
    endpoint: Option<String>,
    bucket: Option<String>,
    key_prefix: Option<String>,
    credentials: Option<aws_sdk_s3::config::Credentials>,
    metrics: Option<Arc<S3Metrics>>,
}

#[common_macros::method_names_consts]
impl S3ContextBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn maybe<T>(self, opt: Option<T>, action: impl FnOnce(Self, T) -> Self) -> Self {
        if let Some(val) = opt {
            action(self, val)
        } else {
            self
        }
    }

    pub fn with_region(self, region: impl Into<String>) -> Self {
        Self {
            region: Some(region.into()),
            ..self
        }
    }

    pub fn with_endpoint(self, endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: Some(endpoint.into()),
            ..self
        }
    }

    pub fn with_bucket(self, bucket: impl Into<String>) -> Self {
        Self {
            bucket: Some(bucket.into()),
            ..self
        }
    }

    pub fn with_key_prefix(self, key_prefix: impl Into<String>) -> Self {
        Self {
            key_prefix: Some(key_prefix.into()),
            ..self
        }
    }

    pub fn with_url(self, url: &Url) -> Self {
        let (endpoint, bucket, key_prefix) = Self::split_url(url);

        assert!(
            key_prefix.is_empty() || key_prefix.ends_with('/'),
            "Base URL does not contain a trailing slash: {url}"
        );

        Self {
            endpoint,
            bucket: Some(bucket),
            key_prefix: Some(key_prefix),
            ..self
        }
    }

    fn split_url(url: &Url) -> (Option<String>, String, String) {
        // TODO: Support virtual hosted style URLs
        // See https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
        let (endpoint, path): (Option<String>, String) =
            match (url.scheme(), url.host_str(), url.port(), url.path()) {
                ("s3", Some(host), None, path) => {
                    return (
                        None,
                        host.to_owned(),
                        path.trim_start_matches('/').to_owned(),
                    );
                }
                ("s3+http", Some(host), None, path) => {
                    (Some(format!("http://{host}")), path.to_owned())
                }
                ("s3+http", Some(host), Some(port), path) => {
                    (Some(format!("http://{host}:{port}")), path.to_owned())
                }
                ("s3+https", Some(host), None, path) => {
                    (Some(format!("https://{host}")), path.to_owned())
                }
                ("s3+https", Some(host), Some(port), path) => {
                    (Some(format!("https://{host}:{port}")), path.to_owned())
                }
                _ => panic!("Unsupported S3 url format: {url}"),
            };

        let (bucket, key_prefix) = match path.trim_start_matches('/').split_once('/') {
            Some((b, p)) => (b.to_owned(), p.to_owned()),
            None => (path.trim_start_matches('/').to_owned(), String::new()),
        };

        (endpoint, bucket, key_prefix)
    }

    pub fn with_credentials(self, credentials: aws_sdk_s3::config::Credentials) -> Self {
        Self {
            credentials: Some(credentials),
            ..self
        }
    }

    pub fn with_credentials_from_keys(self, access_key: &str, secret_key: &str) -> Self {
        let credentials =
            aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "static");

        self.with_credentials(credentials)
    }

    pub fn with_metrics(mut self, metrics: Arc<S3Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    #[tracing::instrument(level = "info", name = S3ContextBuilder_build, skip_all)]
    pub async fn build(self) -> S3Context {
        // SDK Config
        let config_loader = aws_config::defaults(BehaviorVersion::latest());
        let config_loader = if let Some(credentials) = self.credentials {
            config_loader.credentials_provider(credentials)
        } else {
            // Let config loader use default lookups
            config_loader
        };
        let config_loader = if let Some(region) = self.region {
            config_loader.region(aws_config::Region::new(region))
        } else if self.endpoint.is_some() {
            // TODO: This is not ideal, but we usually override endpoint only in tests and
            // don't want to engage the default lookup mechanism which slows down tests
            config_loader.region("unspecified")
        } else {
            // Let config loader use default lookups
            config_loader
        };

        // TODO: Avoid this async load here
        // Consider splitting into speacial `load_defaults` method
        let sdk_config = config_loader.load().await;

        // S3 Config
        let config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);
        let config_builder = if let Some(endpoint) = &self.endpoint {
            config_builder.endpoint_url(endpoint).force_path_style(true)
        } else {
            config_builder
        };
        let s3_config = config_builder.build();

        // Client
        let client = Client::from_conf(s3_config);

        let bucket = self
            .bucket
            .expect("Must specify a bucket name when building S3Context");

        S3Context::new(
            client,
            self.endpoint,
            bucket,
            self.key_prefix.unwrap_or_default(),
            self.metrics,
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
