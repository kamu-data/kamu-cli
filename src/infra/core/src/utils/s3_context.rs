// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use aws_config::{BehaviorVersion, SdkConfig};
use aws_sdk_s3::error::{BoxError, SdkError};
use aws_sdk_s3::operation::delete_object::{DeleteObjectError, DeleteObjectOutput};
use aws_sdk_s3::operation::get_object::{GetObjectError, GetObjectOutput};
use aws_sdk_s3::operation::head_object::{HeadObjectError, HeadObjectOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CommonPrefix, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;
use futures::Stream;
use http_body_util::StreamBody;
use hyper::body::Frame;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::AsyncReadObj;
use tokio::io::ReadBuf;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    pub client: Client,
    pub endpoint: Option<String>,
    pub bucket: String,
    pub key_prefix: String,
    pub sdk_config: SdkConfig,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    const MAX_LISTED_OBJECTS: i32 = 1000;

    pub fn new<S1, S2, S3>(
        client: Client,
        endpoint: Option<S1>,
        bucket: S2,
        key_prefix: S3,
        sdk_config: SdkConfig,
    ) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
        S3: Into<String>,
    {
        Self {
            client,
            endpoint: endpoint.map(Into::into),
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
            sdk_config,
        }
    }

    /// Creates a context for a sub-key while reusing the S3 client and its
    /// credential cache
    pub fn sub_context(&self, sub_key: &str) -> Self {
        let mut key_prefix = self.get_key(sub_key);
        if !key_prefix.ends_with('/') {
            key_prefix.push('/');
        }
        Self {
            client: self.client.clone(),
            endpoint: self.endpoint.clone(),
            bucket: self.bucket.clone(),
            key_prefix,
            sdk_config: self.sdk_config.clone(),
        }
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

        Self::new(client, endpoint, bucket, key_prefix, sdk_config)
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
            .bucket(&self.bucket)
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
            .bucket(&self.bucket)
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
            .bucket(&self.bucket)
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
        let byte_stream = reader_to_bytestream(reader);
        let size = i64::try_from(size).unwrap();

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(byte_stream)
            .content_length(size)
            .send()
            .await
    }

    pub async fn delete_object(
        &self,
        key: String,
    ) -> Result<DeleteObjectOutput, SdkError<DeleteObjectError>> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
    }

    pub async fn bucket_path_exists(&self, key_prefix: &str) -> Result<bool, InternalError> {
        let listing = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
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
            .bucket(&self.bucket)
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
                .bucket(&self.bucket)
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
                    .bucket(&self.bucket)
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
                .bucket(&self.bucket)
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
                        .bucket(&self.bucket)
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
                    .bucket(&self.bucket)
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

struct AsyncReadStream<R: tokio::io::AsyncRead + Unpin + Send + 'static> {
    reader: Arc<tokio::sync::Mutex<R>>,
    buf: Vec<u8>,
}

impl<R: tokio::io::AsyncRead + Unpin + Send + 'static> AsyncReadStream<R> {
    fn new(reader: R) -> Self {
        Self {
            reader: Arc::new(tokio::sync::Mutex::new(reader)),
            buf: vec![0; 8192],
        }
    }
}

impl<R: tokio::io::AsyncRead + Unpin + Send + 'static> Stream for AsyncReadStream<R> {
    type Item = Result<Frame<Bytes>, BoxError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        // Pinning the mutex lock and using a future to get the lock asynchronously
        let mut reader_future = Box::pin(this.reader.lock());

        // Polling the future to get the lock
        use futures::Future;
        let mut reader = futures::ready!(reader_future.as_mut().poll(cx));

        // Reset the buffer before each read
        this.buf.clear();
        this.buf.resize(8192, 0); // Resize to match the buffer size (8KB in this case)

        // Create a ReadBuf from the internal buffer
        let mut read_buf = ReadBuf::new(&mut this.buf);

        // Perform a non-blocking async read
        match futures::ready!(Pin::new(&mut *reader).poll_read(cx, &mut read_buf)) {
            Ok(()) if read_buf.filled().is_empty() => Poll::Ready(None), // No more data to read
            Ok(()) => {
                // Produce a frame with the exact number of bytes read
                let filled_data = Bytes::copy_from_slice(read_buf.filled());
                let frame = Frame::data(filled_data);
                Poll::Ready(Some(Ok(frame)))
            }
            Err(e) => Poll::Ready(Some(Err(BoxError::from(e)))),
        }
    }
}

fn reader_to_bytestream<R: tokio::io::AsyncRead + Unpin + Send + 'static>(reader: R) -> ByteStream {
    let stream = AsyncReadStream::new(reader);
    let byte_stream = StreamBody::new(stream);
    ByteStream::from_body_1_x(byte_stream)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
