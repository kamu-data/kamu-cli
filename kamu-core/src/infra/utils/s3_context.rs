// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rusoto_core::{Region, RusotoError};
use rusoto_s3::*;
use tokio::io::AsyncRead;
use tokio_util::io::ReaderStream;
use url::Url;

use crate::domain::{ErrorIntoInternal, InternalError, ResultIntoInternal};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    pub client: S3Client,
    pub bucket: String,
    pub root_folder_key: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    const MAX_LISTED_OBJECTS: i64 = 1000;

    pub fn new<S1, S2>(client: S3Client, bucket: S1, root_folder_key: S2) -> Self
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

    pub fn from_items(endpoint: Option<String>, bucket: String, root_folder_key: String) -> Self {
        let region = match endpoint {
            None => Region::default(),
            Some(endpoint) => Region::Custom {
                name: "custom".to_owned(),
                endpoint: endpoint,
            },
        };
        Self::new(S3Client::new(region), bucket, root_folder_key)
    }

    pub fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, root_folder_key) = Self::split_url(url);
        Self::from_items(endpoint, bucket, root_folder_key)
    }

    pub fn split_url(url: &Url) -> (Option<String>, String, String) {
        // TODO: Support virtual hosted style URLs once rusoto supports them
        // See: https://github.com/rusoto/rusoto/issues/1482
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
    ) -> Result<HeadObjectOutput, RusotoError<HeadObjectError>> {
        self.client
            .head_object(HeadObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..HeadObjectRequest::default()
            })
            .await
    }

    pub async fn get_object(
        &self,
        key: String,
    ) -> Result<GetObjectOutput, RusotoError<GetObjectError>> {
        self.client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..GetObjectRequest::default()
            })
            .await
    }

    pub async fn put_object(
        &self,
        key: String,
        data: &[u8],
    ) -> Result<PutObjectOutput, RusotoError<PutObjectError>> {
        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key,
                // TODO: PERF: Avoid copying data into a buffer
                body: Some(rusoto_core::ByteStream::from(Vec::from(data))),
                content_length: Some(data.len() as i64),
                ..PutObjectRequest::default()
            })
            .await
    }

    pub async fn put_object_stream(
        &self,
        key: String,
        stream: ReaderStream<Box<AsyncReadObj>>,
        size: i64,
    ) -> Result<PutObjectOutput, RusotoError<PutObjectError>> {
        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key,
                // TODO: PERF: Avoid copying data into a buffer
                body: Some(rusoto_core::ByteStream::new(stream)),
                content_length: Some(size),
                ..PutObjectRequest::default()
            })
            .await
    }

    pub async fn delete_object(
        &self,
        key: String,
    ) -> Result<DeleteObjectOutput, RusotoError<DeleteObjectError>> {
        self.client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..DeleteObjectRequest::default()
            })
            .await
    }

    pub async fn bucket_path_exists(&self, key_prefix: &str) -> Result<bool, InternalError> {
        let listing = self
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                prefix: Some(self.get_key(key_prefix)),
                max_keys: Some(1),
                ..ListObjectsV2Request::default()
            })
            .await;

        match listing {
            Ok(resp) => Ok(resp.contents.is_some()),
            Err(e) => Err(e.int_err().into()),
        }
    }

    pub async fn bucket_list_folders(&self) -> Result<Vec<CommonPrefix>, InternalError> {
        let list_objects_resp = self
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                delimiter: Some("/".to_owned()),
                ..ListObjectsV2Request::default()
            })
            .await
            .int_err()?;

        // TODO: Support iteration
        assert!(
            !list_objects_resp.is_truncated.unwrap_or(false),
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
                .list_objects_v2(ListObjectsV2Request {
                    bucket: self.bucket.clone(),
                    prefix: Some(key_prefix.clone()),
                    max_keys: Some(S3Context::MAX_LISTED_OBJECTS),
                    ..ListObjectsV2Request::default()
                })
                .await
                .int_err()?;

            if let Some(contents) = list_response.contents {
                has_next_page = (contents.len() as i64) == S3Context::MAX_LISTED_OBJECTS;
                self.client
                    .delete_objects(DeleteObjectsRequest {
                        bucket: self.bucket.clone(),
                        delete: Delete {
                            objects: contents
                                .iter()
                                .map(|obj| ObjectIdentifier {
                                    key: obj.key.clone().unwrap(),
                                    version_id: None,
                                })
                                .collect(),
                            quiet: Some(true),
                        },
                        ..DeleteObjectsRequest::default()
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
                .client
                .list_objects_v2(ListObjectsV2Request {
                    bucket: self.bucket.clone(),
                    prefix: Some(old_key_prefix.clone()),
                    max_keys: Some(S3Context::MAX_LISTED_OBJECTS),
                    ..ListObjectsV2Request::default()
                })
                .await
                .int_err()?;

            // TODO: concurrency safety.
            // It is important not to allow parallel writes of Head reference file in the same bucket.
            // Consider optimistic locking (comparing old head with expected before final commit).

            if let Some(contents) = list_response.contents {
                has_next_page = (contents.len() as i64) == S3Context::MAX_LISTED_OBJECTS;
                for obj in &contents {
                    let copy_source =
                        format!("{}/{}", self.bucket.clone(), obj.key.clone().unwrap());
                    let new_key = obj
                        .key
                        .clone()
                        .unwrap()
                        .replace(old_key_prefix.as_str(), new_key_prefix.as_str());
                    self.client
                        .copy_object(CopyObjectRequest {
                            bucket: self.bucket.clone(),
                            copy_source,
                            key: new_key,
                            ..CopyObjectRequest::default()
                        })
                        .await
                        .int_err()?;
                }

                self.client
                    .delete_objects(DeleteObjectsRequest {
                        bucket: self.bucket.clone(),
                        delete: Delete {
                            objects: contents
                                .iter()
                                .map(|obj| ObjectIdentifier {
                                    key: obj.key.clone().unwrap(),
                                    version_id: None,
                                })
                                .collect(),
                            quiet: Some(true),
                        },
                        ..DeleteObjectsRequest::default()
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

/////////////////////////////////////////////////////////////////////////////////////////
