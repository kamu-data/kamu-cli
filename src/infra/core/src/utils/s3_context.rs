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

use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::config::SharedCredentialsProvider;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::types::{CommonPrefix, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use internal_error::{InternalError, ResultIntoInternal, *};
use kamu_core::AsyncReadObj;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    client: Client,
    endpoint: Option<Arc<str>>,
    bucket: Arc<str>,
    key_prefix: Arc<str>,
    sdk_config: Arc<SdkConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    const MAX_LISTED_OBJECTS: i32 = 1000;

    #[inline]
    pub fn client(&self) -> &Client {
        &self.client
    }

    #[inline]
    pub fn endpoint(&self) -> Option<&str> {
        self.endpoint.as_deref()
    }

    #[inline]
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    #[inline]
    pub fn key_prefix(&self) -> &str {
        &self.key_prefix
    }

    pub fn new(
        client: Client,
        endpoint: Option<impl AsRef<str>>,
        bucket: impl AsRef<str>,
        key_prefix: impl AsRef<str>,
        sdk_config: Arc<SdkConfig>,
    ) -> Self {
        Self {
            client,
            endpoint: endpoint.map(|s| s.as_ref().into()),
            bucket: bucket.as_ref().into(),
            key_prefix: key_prefix.as_ref().into(),
            sdk_config,
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
        self.key_prefix = key_prefix.into();
        self
    }

    pub fn credentials_provider(&self) -> Option<SharedCredentialsProvider> {
        self.sdk_config.credentials_provider()
    }

    #[tracing::instrument(level = "info", name = "init_s3_context")]
    pub async fn from_items(endpoint: Option<String>, bucket: String, key_prefix: String) -> Self {
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

        Self::new(client, endpoint, bucket, key_prefix, Arc::new(sdk_config))
    }

    pub async fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, key_prefix) = Self::split_url(url);

        assert!(
            key_prefix.is_empty() || key_prefix.ends_with('/'),
            "Base URL does not contain a trailing slash: {url}"
        );

        Self::from_items(endpoint, bucket, key_prefix).await
    }

    pub fn split_url(url: &Url) -> (Option<String>, String, String) {
        // TODO: Support virtual hosted style URLs
        // See https://aws.amazon.com/blogs/aws/amazon-s3-path-deprecation-plan-the-rest-of-the-story/
        let (endpoint, path): (Option<String>, String) =
            match (url.scheme(), url.host_str(), url.port(), url.path()) {
                ("s3", Some(host), None, path) => {
                    return (
                        None,
                        host.to_owned(),
                        path.trim_start_matches('/').to_owned(),
                    )
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

    pub fn make_url(&self) -> Url {
        let context_url_str = match &self.endpoint {
            Some(endpoint) => {
                format!("s3+{}/{}/{}", endpoint, self.bucket, self.key_prefix)
            }
            None => {
                format!("s3://{}/{}", self.bucket, self.key_prefix)
            }
        };
        Url::parse(context_url_str.as_str()).unwrap()
    }

    pub fn region(&self) -> Option<&str> {
        self.client.config().region().map(AsRef::as_ref)
    }

    pub fn get_key(&self, sub_key: &str) -> String {
        if self.key_prefix.is_empty() {
            String::from(sub_key)
        } else {
            format!("{}{}", self.key_prefix, sub_key)
        }
    }

    pub async fn head_object(
        &self,
        key: String,
    ) -> Result<HeadObjectOutput, SdkError<HeadObjectError>> {
        self.client
            .head_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
    }

    pub async fn get_object(
        &self,
        key: String,
    ) -> Result<GetObjectOutput, SdkError<GetObjectError>> {
        self.client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
    }

    pub async fn put_object(
        &self,
        key: String,
        data: &[u8],
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        let size = i64::try_from(data.len()).unwrap();

        self.client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            // TODO: PERF: Avoid copying data into a buffer
            .body(Vec::from(data).into())
            .content_length(size)
            .send()
            .await
    }

    pub async fn put_object_stream(
        &self,
        key: String,
        reader: Box<AsyncReadObj>,
        size: u64,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        use aws_smithy_types::body::SdkBody;
        use aws_smithy_types::byte_stream::ByteStream;

        // FIXME: https://github.com/awslabs/aws-sdk-rust/issues/1030
        let stream = tokio_util::io::ReaderStream::new(reader);
        let body = reqwest::Body::wrap_stream(stream);
        let byte_stream = ByteStream::new(SdkBody::from_body_1_x(body));

        self.client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .body(byte_stream)
            .content_length(i64::try_from(size).unwrap())
            .send()
            .await
    }

    pub async fn delete_object(
        &self,
        key: String,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
        self.client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
    }

    pub async fn bucket_path_exists(&self, key_prefix: &str) -> Result<bool, InternalError> {
        let listing = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.as_ref())
            .prefix(self.get_key(key_prefix))
            .max_keys(1)
            .send()
            .await;

        match listing {
            Ok(resp) => Ok(resp.contents.is_some()),
            Err(e) => Err(e.int_err()),
        }
    }

    pub async fn bucket_list_folders(&self) -> Result<Vec<CommonPrefix>, InternalError> {
        let list_objects_resp = self
            .client
            .list_objects_v2()
            .bucket(self.bucket.as_ref())
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
    }

    pub async fn recursive_delete(&self, key_prefix: String) -> Result<(), InternalError> {
        // ListObjectsV2Request returns at most S3Context::MAX_LISTED_OBJECTS=1000 items
        let mut has_next_page = true;
        while has_next_page {
            let list_response = self
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(&key_prefix)
                .max_keys(Self::MAX_LISTED_OBJECTS)
                .send()
                .await
                .int_err()?;

            if let Some(contents) = list_response.contents {
                let object_identifiers = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect::<Result<Vec<_>, _>>()
                    .int_err()?;

                has_next_page = list_response.is_truncated.unwrap_or_default();
                self.client
                    .delete_objects()
                    .bucket(self.bucket.as_ref())
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .quiet(true)
                            .build()
                            .int_err()?,
                    )
                    .send()
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
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(&old_key_prefix)
                .max_keys(Self::MAX_LISTED_OBJECTS)
                .send()
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
                        format!("{}/{}", self.bucket.clone(), obj.key.clone().unwrap());
                    let new_key = obj
                        .key
                        .clone()
                        .unwrap()
                        .replace(old_key_prefix.as_str(), new_key_prefix.as_str());
                    self.client
                        .copy_object()
                        .bucket(self.bucket.as_ref())
                        .copy_source(copy_source)
                        .key(new_key)
                        .send()
                        .await
                        .int_err()?;
                }

                let object_identifiers = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect::<Result<Vec<_>, _>>()
                    .int_err()?;

                self.client
                    .delete_objects()
                    .bucket(self.bucket.as_ref())
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .quiet(true)
                            .build()
                            .int_err()?,
                    )
                    .send()
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
