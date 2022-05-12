// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::repos::named_object_repository::{DeleteError, GetError, SetError};
use crate::domain::*;

use async_trait::async_trait;
use bytes::Bytes;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::*;
use tracing::debug;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryS3 {
    client: S3Client,
    bucket: String,
    key_prefix: String,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryS3 {
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
        // TODO: Support virtual hosted style URLs once rusoto supports them
        // See: https://github.com/rusoto/rusoto/issues/1482
        let (endpoint, path): (Option<String>, String) =
            match (url.scheme(), url.host_str(), url.port(), url.path()) {
                ("s3", Some(host), None, "") => (None, host.to_owned()),
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

        debug!(?endpoint, ?bucket, ?key_prefix, "Creating S3 client");

        Self::from_items(endpoint, bucket, key_prefix)
    }

    fn get_key(&self, name: &str) -> String {
        if self.key_prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{}{}", self.key_prefix, name)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryS3 {
    async fn get(&self, name: &str) -> Result<Bytes, GetError> {
        let key = self.get_key(name);

        debug!(?key, "Reading object stream");

        let resp = match self
            .client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..GetObjectRequest::default()
            })
            .await
        {
            Ok(resp) => Ok(resp),
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => {
                Err(GetError::NotFound(NotFoundError {
                    name: name.to_owned(),
                }))
            }
            Err(e @ RusotoError::Credentials(_)) => {
                Err(GetError::Access(AccessError::Unauthorized(e.into())))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        let mut stream = resp.body.expect("Response with no body").into_async_read();

        use tokio::io::AsyncReadExt;
        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.int_err()?;

        Ok(Bytes::from(data))
    }

    async fn set(&self, name: &str, data: &[u8]) -> Result<(), SetError> {
        let key = self.get_key(name);

        debug!(?key, "Inserting object");

        // TODO: PERF: Avoid copying data into a buffer
        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key,
                body: Some(rusoto_core::ByteStream::from(Vec::from(data))),
                ..PutObjectRequest::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e @ RusotoError::Credentials(_)) => {
                Err(SetError::Access(AccessError::Unauthorized(e.into())))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        Ok(())
    }

    async fn delete(&self, name: &str) -> Result<(), DeleteError> {
        let key = self.get_key(name);

        debug!(?key, "Deleting object");

        match self
            .client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..DeleteObjectRequest::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e @ RusotoError::Credentials(_)) => {
                Err(DeleteError::Access(AccessError::Unauthorized(e.into())))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        Ok(())
    }
}
