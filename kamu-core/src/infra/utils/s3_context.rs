// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::{
    delete_object::{DeleteObjectError, DeleteObjectOutput},
    get_object::{GetObjectError, GetObjectOutput},
    head_object::{HeadObjectError, HeadObjectOutput},
    put_object::{PutObjectError, PutObjectOutput},
};
use aws_sdk_s3::types::{CommonPrefix, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use aws_smithy_http::byte_stream::ByteStream;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::domain::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    pub client: Client,
    pub bucket: String,
    pub root_folder_key: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    const MAX_LISTED_OBJECTS: i32 = 1000;

    pub fn new<S1, S2>(client: Client, bucket: S1, root_folder_key: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            client,
            bucket: bucket.into(),
            root_folder_key: root_folder_key.into(),
        }
    }

    pub async fn from_items(
        endpoint: Option<String>,
        bucket: String,
        root_folder_key: String,
    ) -> Self {
        // Note: Falling back to `unspecified` region as SDK errors out when the region not set
        // even if using custom endpoint
        let region_provider = aws_config::meta::region::RegionProviderChain::default_provider()
            .or_else("unspecified");
        let sdk_config = aws_config::from_env().region(region_provider).load().await;
        let s3_config = if let Some(endpoint) = endpoint {
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .endpoint_url(endpoint)
                .force_path_style(true)
                .build()
        } else {
            aws_sdk_s3::config::Builder::from(&sdk_config).build()
        };

        // TODO: PERF: Client construction is expensive and should only be done once
        let client = Client::from_conf(s3_config);

        Self::new(client, bucket, root_folder_key)
    }

    pub async fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, root_folder_key) = Self::split_url(url);

        assert!(
            root_folder_key.is_empty() || root_folder_key.ends_with('/'),
            "Base URL does not contain a trailing slash: {}",
            url
        );

        Self::from_items(endpoint, bucket, root_folder_key).await
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
                    (Some(format!("http://{}", host)), path.to_owned())
                }
                ("s3+http", Some(host), Some(port), path) => {
                    (Some(format!("http://{}:{}", host, port)), path.to_owned())
                }
                ("s3+https", Some(host), None, path) => {
                    (Some(format!("https://{}", host)), path.to_owned())
                }
                ("s3+https", Some(host), Some(port), path) => {
                    (Some(format!("https://{}:{}", host, port)), path.to_owned())
                }
                _ => panic!("Unsupported S3 url format: {}", url),
            };

        let (bucket, root_folder_key) = match path.trim_start_matches('/').split_once('/') {
            Some((b, p)) => (b.to_owned(), p.to_owned()),
            None => (path.trim_start_matches('/').to_owned(), String::new()),
        };

        (endpoint, bucket, root_folder_key)
    }

    pub fn get_key(&self, key: &str) -> String {
        if self.root_folder_key.is_empty() {
            String::from(key)
        } else {
            format!("{}{}", self.root_folder_key, key)
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
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            // TODO: PERF: Avoid copying data into a buffer
            .body(ByteStream::from(Vec::from(data)))
            .content_length(data.len() as i64)
            .send()
            .await
    }

    pub async fn put_object_stream(
        &self,
        key: String,
        stream: ReaderStream<Box<AsyncReadObj>>,
        size: i64,
    ) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
        let body = hyper::Body::wrap_stream(stream);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
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
            Err(e) => Err(e.int_err().into()),
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
            !list_objects_resp.is_truncated,
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
                let object_identifiers: Vec<_> = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect();

                has_next_page = list_response.is_truncated;
                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .quiet(true)
                            .build(),
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
            // It is important not to allow parallel writes of Head reference file in the same bucket.
            // Consider optimistic locking (comparing old head with expected before final commit).

            has_next_page = list_response.is_truncated();
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

                let object_identifiers: Vec<_> = contents
                    .into_iter()
                    .map(|obj| ObjectIdentifier::builder().key(obj.key().unwrap()).build())
                    .collect();

                self.client
                    .delete_objects()
                    .bucket(&self.bucket)
                    .delete(
                        Delete::builder()
                            .set_objects(Some(object_identifiers))
                            .quiet(true)
                            .build(),
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

/////////////////////////////////////////////////////////////////////////////////////////
