// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rusoto_core::Region;
use rusoto_s3::{
    CommonPrefix, CopyObjectRequest, Delete, DeleteObjectsRequest, ListObjectsV2Request,
    ObjectIdentifier, S3Client, S3,
};
use url::Url;

use crate::domain::{ErrorIntoInternal, InternalError, ResultIntoInternal};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct S3Context {
    pub client: S3Client,
    pub bucket: String,
    pub key_prefix: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl S3Context {
    pub fn new<S1, S2>(client: S3Client, bucket: S1, key_prefix: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            client,
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
        }
    }

    pub fn from_items(endpoint: Option<String>, bucket: String, key_prefix: String) -> Self {
        let region = match endpoint {
            None => Region::default(),
            Some(endpoint) => Region::Custom {
                name: "custom".to_owned(),
                endpoint: endpoint,
            },
        };
        Self::new(S3Client::new(region), bucket, key_prefix)
    }

    pub fn from_url(url: &Url) -> Self {
        let (endpoint, bucket, key_prefix) = Self::split_url(url);
        Self::from_items(endpoint, bucket, key_prefix)
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

        let (bucket, key_prefix) = match path.trim_start_matches('/').split_once('/') {
            Some((b, p)) => (b.to_owned(), p.to_owned()),
            None => (path.trim_start_matches('/').to_owned(), String::new()),
        };

        (endpoint, bucket, key_prefix)
    }

    pub fn get_key(&self, key: &str) -> String {
        if self.key_prefix.is_empty() {
            String::from(key)
        } else {
            format!("{}{}", self.key_prefix, key)
        }
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

        Ok(list_objects_resp.common_prefixes.unwrap_or_default())
    }

    pub async fn recursive_delete(&self, key_prefix: String) -> Result<(), InternalError> {
        let list_response = self
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                prefix: Some(key_prefix),
                ..ListObjectsV2Request::default()
            })
            .await
            .int_err()?;

        if let Some(contents) = list_response.contents {
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
        }

        Ok(())
    }

    pub async fn recursive_move(
        &self,
        old_key_prefix: String,
        new_key_prefix: String,
    ) -> Result<(), InternalError> {
        let list_response = self
            .client
            .list_objects_v2(ListObjectsV2Request {
                bucket: self.bucket.clone(),
                prefix: Some(old_key_prefix.clone()),
                ..ListObjectsV2Request::default()
            })
            .await
            .int_err()?;

        // TODO: concurrency safety.
        // It is important not to allow parallel writes of Head reference file in the same bucket.
        // Consider optimistic locking (comparing old head with expected before final commit).

        if let Some(contents) = list_response.contents {
            for obj in &contents {
                let copy_source = format!("{}/{}", self.bucket.clone(), obj.key.clone().unwrap());
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
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
