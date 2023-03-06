// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;
use chrono::Utc;
use opendatafabric::{Multicodec, Multihash};

use async_trait::async_trait;
use bytes::Bytes;
use rusoto_core::{
    credential::{ChainProvider, ProvideAwsCredentials},
    Region, RusotoError,
};
use rusoto_s3::{
    util::{PreSignedRequest, PreSignedRequestOption},
    *,
};
use std::{marker::PhantomData, path::Path};
use tokio::io::AsyncRead;
use tracing::debug;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncReadObj = dyn AsyncRead + Send + Unpin;

/////////////////////////////////////////////////////////////////////////////////////////

// TODO: Pass a single type that configures digest algo, multicodec, and hash base
// TODO: Verify atomic behavior
pub struct ObjectRepositoryS3<D, const C: u32> {
    client: S3Client,
    bucket: String,
    key_prefix: String,
    _phantom: PhantomData<D>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl<D, const C: u32> ObjectRepositoryS3<D, C> {
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
}

impl<D, const C: u32> ObjectRepositoryS3<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    pub fn new<S1, S2>(client: S3Client, bucket: S1, key_prefix: S2) -> Self
    where
        S1: Into<String>,
        S2: Into<String>,
    {
        Self {
            client,
            bucket: bucket.into(),
            key_prefix: key_prefix.into(),
            _phantom: PhantomData,
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

    fn get_key(&self, hash: &Multihash) -> String {
        if self.key_prefix.is_empty() {
            hash.to_multibase_string()
        } else {
            format!("{}{}", self.key_prefix, hash)
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl<D, const C: u32> ObjectRepository for ObjectRepositoryS3<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let key = self.get_key(hash);

        debug!(?key, "Checking for object");

        match self
            .client
            .head_object(HeadObjectRequest {
                bucket: self.bucket.clone(),
                key,
                ..HeadObjectRequest::default()
            })
            .await
        {
            Ok(_) => {
                return Ok(true);
            }
            // TODO: This error type doesn't work
            // See: https://github.com/rusoto/rusoto/issues/716
            Err(RusotoError::Service(HeadObjectError::NoSuchKey(_))) => Ok(false),
            Err(e @ RusotoError::Credentials(_)) => Err(AccessError::Unauthorized(e.into()).into()),
            Err(_) => Ok(false), // return Err(e.into()),
        }
    }

    async fn get_size(&self, _hash: &Multihash) -> Result<u64, GetError> {
        panic!("get_size unsupported for S3 object repository");
    }

    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        use tokio::io::AsyncReadExt;
        let mut stream = self.get_stream(hash).await?;

        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.int_err()?;

        Ok(Bytes::from(data))
    }

    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let key = self.get_key(hash);

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
                Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                }))
            }
            Err(e @ RusotoError::Credentials(_)) => Err(AccessError::Unauthorized(e.into()).into()),
            Err(e) => Err(e.int_err().into()),
        }?;

        let stream = resp.body.expect("Response with no body").into_async_read();
        Ok(Box::new(stream))
    }

    async fn get_download_url(
        &self,
        hash: &Multihash,
        opts: DownloadOpts,
    ) -> Result<GetDownloadUrlResult, GetDownloadUrlError> {
        let key = self.get_key(hash);
        let get_object_request = GetObjectRequest {
            bucket: self.bucket.clone(),
            key,
            ..GetObjectRequest::default()
        };

        let provider = ChainProvider::new();
        let credentials = provider.credentials().await.unwrap();

        let validity_period_seconds: i64 = match opts.expiration {
            Some(expiration) => expiration.num_seconds(),
            None => 3600, /* default expiration */
        };
        let options = PreSignedRequestOption {
            expires_in: std::time::Duration::from_secs(validity_period_seconds as u64),
        };

        let presigned_url =
            get_object_request.get_presigned_url(&(Region::default()), &credentials, &options);
        match Url::parse(presigned_url.as_str()) {
            Ok(url) => Ok(GetDownloadUrlResult {
                url,
                expires_at: Some(Utc::now() + chrono::Duration::seconds(validity_period_seconds)),
            }),
            Err(e) => Err(GetDownloadUrlError::Internal(e.int_err())),
        }
    }

    async fn insert_bytes<'a>(
        &'a self,
        data: &'a [u8],
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            Multihash::from_digest::<D>(Multicodec::try_from(C).unwrap(), data)
        };

        if let Some(expected_hash) = options.expected_hash {
            if *expected_hash != hash {
                return Err(InsertError::HashMismatch(HashMismatchError {
                    expected: expected_hash.clone(),
                    actual: hash,
                }));
            }
        }

        let key = self.get_key(&hash);

        debug!(?key, "Inserting object");

        // TODO: PERF: Avoid copying data into a buffer
        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key,
                body: Some(rusoto_core::ByteStream::from(Vec::from(data))),
                content_length: Some(data.len() as i64),
                ..PutObjectRequest::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e @ RusotoError::Credentials(_)) => {
                Err(InsertError::Access(AccessError::Unauthorized(e.into())))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        Ok(InsertResult {
            hash,
            already_existed: false,
        })
    }

    async fn insert_stream<'a>(
        &'a self,
        src: Box<AsyncReadObj>,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let hash = if let Some(hash) = options.precomputed_hash {
            hash.clone()
        } else {
            panic!("Writing steam into s3 only supports pre-computed hashes")
        };

        let size = if let Some(size) = options.size_hint {
            size
        } else {
            panic!("Writing stream into s3 requires knowing the total size (until we implement multi-part uploads)")
        };

        let key = self.get_key(&hash);

        debug!(?key, size, "Inserting object stream");

        if self.contains(&hash).await? {
            return Ok(InsertResult {
                hash,
                already_existed: true,
            });
        }

        use tokio_util::io::ReaderStream;
        let stream = ReaderStream::new(src);

        match self
            .client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key,
                body: Some(rusoto_core::ByteStream::new(stream)),
                content_length: Some(size as i64),
                ..PutObjectRequest::default()
            })
            .await
        {
            Ok(_) => Ok(()),
            Err(e @ RusotoError::Credentials(_)) => {
                Err(InsertError::Access(AccessError::Unauthorized(e.into())))
            }
            Err(e) => Err(e.int_err().into()),
        }?;

        Ok(InsertResult {
            hash,
            already_existed: false,
        })
    }

    async fn insert_file_move<'a>(
        &'a self,
        _src: &Path,
        _options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        unimplemented!()
    }

    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let key = self.get_key(&hash);

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
