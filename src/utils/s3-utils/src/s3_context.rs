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
use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::error::SdkError;
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
    sdk_config: SdkConfig,
    bucket_url: String,
}

impl S3ContextSharedState {
    fn new(endpoint: Option<String>, bucket: String, sdk_config: SdkConfig) -> Self {
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
            sdk_config,
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

    pub fn new(
        client: Client,
        endpoint: Option<impl Into<String>>,
        bucket: impl Into<String>,
        key_prefix: impl Into<String>,
        sdk_config: SdkConfig,
    ) -> Self {
        let endpoint = endpoint.map(Into::into);
        let bucket = bucket.into();
        let key_prefix = key_prefix.into();

        let url = Self::make_url(endpoint.as_deref(), &bucket, &key_prefix);

        Self {
            client,
            shared_state: Arc::new(S3ContextSharedState::new(endpoint, bucket, sdk_config)),
            state: Arc::new(S3ContextState { key_prefix, url }),
            maybe_metrics: None,
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

    pub fn with_metrics(mut self, metrics: Arc<S3Metrics>) -> Self {
        self.maybe_metrics = Some(metrics);
        self
    }

    pub fn credentials_provider(&self) -> Option<SharedCredentialsProvider> {
        self.shared_state.sdk_config.credentials_provider()
    }

    async fn from_items(endpoint: Option<String>, bucket: String, key_prefix: String) -> Self {
        // Note: Falling back to `unspecified` region as SDK errors out when the region
        // not set even if using custom endpoint
        let region_provider = aws_config::meta::region::RegionProviderChain::default_provider()
            .or_else("unspecified");

        let sdk_config = aws_config::defaults(BehaviorVersion::latest())
            .region(region_provider)
            .load()
            .await;

        let s3_config = if let Some(endpoint) = endpoint.clone() {
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .endpoint_url(endpoint)
                .force_path_style(true)
                .build()
        } else {
            aws_sdk_s3::config::Builder::from(&sdk_config).build()
        };

        // TODO: PERF: Client construction is expensive and should only be done once
        let client = Client::from_conf(s3_config);

        Self::new(client, endpoint, bucket, key_prefix, sdk_config)
    }

    #[tracing::instrument(level = "info", name = S3Context_from_url)]
    pub async fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, key_prefix) = Self::split_url(url);

        assert!(
            key_prefix.is_empty() || key_prefix.ends_with('/'),
            "Base URL does not contain a trailing slash: {url}"
        );

        Self::from_items(endpoint, bucket, key_prefix).await
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
            let list_objects_resp = self
                .client
                .list_objects_v2()
                .bucket(self.shared_state.bucket.clone())
                .delimiter("/")
                .send()
                .await
                .int_err()?;

            // TODO: Support iteration
            assert!(
                !list_objects_resp.is_truncated.unwrap_or_default(),
                "Cannot handle truncated response"
            );

            Ok(list_objects_resp.common_prefixes.unwrap_or_default())
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
