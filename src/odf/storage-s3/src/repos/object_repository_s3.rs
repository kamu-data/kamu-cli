// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;
use std::marker::PhantomData;
use std::path::Path;

use async_trait::async_trait;
use async_utils::AsyncReadObj;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::presigning::PresigningConfig;
use bytes::Bytes;
use internal_error::*;
use odf_metadata::*;
use odf_storage::*;
use s3_utils::{GetObjectOptions, PutObjectOptions, S3Context};
use tokio::io::AsyncReadExt;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DEFAULT_EXPIRES_IN: chrono::TimeDelta = chrono::Duration::seconds(3600);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type ObjectRepositoryS3Sha3 =
    ObjectRepositoryS3<sha3::Sha3_256, { Multicodec::Sha3_256 as u32 }>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Pass a single type that configures digest algo, multicodec, and hash
//       base
// TODO: Verify atomic behavior
pub struct ObjectRepositoryS3<D, const C: u32> {
    s3_context: S3Context,
    _phantom: PhantomData<D>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl<D, const C: u32> ObjectRepositoryS3<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    pub fn new(s3_context: S3Context) -> Self {
        Self {
            s3_context,
            _phantom: PhantomData,
        }
    }

    fn get_key(&self, hash: &Multihash) -> String {
        self.s3_context
            .get_key(&hash.as_multibase().to_stack_string())
    }

    fn into_header_map<'a>(iter: impl Iterator<Item = (&'a str, &'a str)>) -> http::HeaderMap {
        iter.map(|(k, v)| {
            (
                http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                http::HeaderValue::from_str(v).unwrap(),
            )
        })
        .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl<D, const C: u32> ObjectRepository for ObjectRepositoryS3<D, C>
where
    D: Send + Sync,
    D: digest::Digest,
{
    fn protocol(&self) -> ObjectRepositoryProtocol {
        ObjectRepositoryProtocol::S3
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_contains, skip_all, fields(%hash))]
    async fn contains(&self, hash: &Multihash) -> Result<bool, ContainsError> {
        let key = self.get_key(hash);

        tracing::debug!(?key, "Checking for object");

        match self.s3_context.head_object(key).await {
            Ok(_) => Ok(true),
            Err(err) => match err.into_service_error() {
                // TODO: Detect credentials error
                HeadObjectError::NotFound(_) => Ok(false),
                err => return Err(err.int_err().into()),
            },
        }
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_get_size, skip_all, fields(%hash))]
    async fn get_size(&self, hash: &Multihash) -> Result<u64, GetError> {
        let key = self.get_key(hash);

        tracing::debug!(?key, "Checking for object");

        match self.s3_context.head_object(key).await {
            Ok(output) => u64::try_from(
                output
                    .content_length
                    .ok_or_else(|| "S3 did not return content length".int_err())?,
            )
            .map_err(|err| err.int_err().into()),
            Err(err) => match err.into_service_error() {
                // TODO: Detect credentials error
                HeadObjectError::NotFound(_) => Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                })),
                err => Err(err.int_err().into()),
            },
        }
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_get_bytes, skip_all, fields(%hash))]
    async fn get_bytes(&self, hash: &Multihash) -> Result<Bytes, GetError> {
        let key = self.get_key(hash);

        tracing::debug!(?key, "Reading object");

        let resp = match self.s3_context.get_object(key).await {
            Ok(resp) => Ok(resp),
            Err(err) => match err.into_service_error() {
                // TODO: Detect credentials error
                GetObjectError::NoSuchKey(_) => Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                })),
                err => return Err(err.int_err().into()),
            },
        }?;

        // Allocate the exact sized buffer for the body
        let mut buf = Vec::with_capacity(
            usize::try_from(
                resp.content_length()
                    .ok_or_else(|| "S3 did not return content length".int_err())?,
            )
            .int_err()?,
        );

        let mut body = resp.body.into_async_read();
        body.read_to_end(&mut buf).await.int_err()?;

        Ok(Bytes::from(buf))
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_get_stream, skip_all, fields(%hash))]
    async fn get_stream(&self, hash: &Multihash) -> Result<Box<AsyncReadObj>, GetError> {
        let key = self.get_key(hash);

        tracing::debug!(?key, "Reading object stream");

        let resp = match self.s3_context.get_object(key).await {
            Ok(resp) => Ok(resp),
            Err(err) => match err.into_service_error() {
                // TODO: Detect credentials error
                GetObjectError::NoSuchKey(_) => Err(GetError::NotFound(ObjectNotFoundError {
                    hash: hash.clone(),
                })),
                err => return Err(err.int_err().into()),
            },
        }?;

        let stream = resp.body.into_async_read();
        Ok(Box::new(stream))
    }

    async fn get_internal_url(&self, hash: &Multihash) -> Url {
        // TODO: This URL does not account for endpoint and it will collide in case we
        // work with multiple S3-like storages having same buckets names
        let context_url = Url::parse(
            format!(
                "s3://{}/{}",
                self.s3_context.bucket(),
                self.s3_context.key_prefix()
            )
            .as_str(),
        )
        .unwrap();

        context_url
            .join(&hash.as_multibase().to_stack_string())
            .unwrap()
    }

    async fn get_external_download_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        let expires_in = opts.expiration.unwrap_or(DEFAULT_EXPIRES_IN);

        let presigned_config = PresigningConfig::builder()
            .expires_in(expires_in.to_std().unwrap())
            .build()
            .expect("Invalid presigning config");

        let expires_at = presigned_config.start_time() + presigned_config.expires();
        let presigned_request = self
            .s3_context
            .get_object_presigned_request(
                self.get_key(hash),
                GetObjectOptions::builder()
                    .presigned_config(presigned_config)
                    .build(),
            )
            .await
            .int_err()?;
        let presigned_request_url = Url::parse(presigned_request.uri()).int_err()?;

        Ok(GetExternalUrlResult {
            url: presigned_request_url,
            header_map: Self::into_header_map(presigned_request.headers()),
            expires_at: Some(expires_at.into()),
        })
    }

    async fn get_external_upload_url(
        &self,
        hash: &Multihash,
        opts: ExternalTransferOpts,
    ) -> Result<GetExternalUrlResult, GetExternalUrlError> {
        let expires_in = opts.expiration.unwrap_or(DEFAULT_EXPIRES_IN);

        let presigned_config = PresigningConfig::builder()
            .expires_in(expires_in.to_std().unwrap())
            .build()
            .expect("Invalid presigning config");

        let expires_at = presigned_config.start_time() + presigned_config.expires();
        let presigned_request = self
            .s3_context
            .put_object_presigned_request(
                self.get_key(hash),
                PutObjectOptions::builder()
                    .presigned_config(presigned_config)
                    .build(),
            )
            .await
            .int_err()?;
        let presigned_request_url = Url::parse(presigned_request.uri()).int_err()?;

        Ok(GetExternalUrlResult {
            url: presigned_request_url,
            header_map: Self::into_header_map(presigned_request.headers()),
            expires_at: Some(expires_at.into()),
        })
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_insert_bytes, skip_all)]
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

        tracing::debug!(?key, "Inserting object");

        self.s3_context
            .put_object(key, data)
            .await
            // TODO: Detect credentials error
            .map_err(|e| e.into_service_error().int_err())?;

        Ok(InsertResult { hash })
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_insert_stream, skip_all)]
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

        let Some(size) = options.size_hint else {
            panic!(
                "Writing stream into s3 requires knowing the total size (until we implement \
                 multi-part uploads)"
            )
        };

        let key = self.get_key(&hash);

        tracing::debug!(?key, size, "Inserting object stream");

        self.s3_context
            .put_object_stream(key, src, size)
            .await
            // TODO: Detect credentials error
            .map_err(|e| e.into_service_error().int_err())?;

        Ok(InsertResult { hash })
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_insert_file_move, skip_all)]
    async fn insert_file_move<'a>(
        &'a self,
        src: &Path,
        options: InsertOpts<'a>,
    ) -> Result<InsertResult, InsertError> {
        let file = tokio::fs::File::open(src).await.int_err()?;
        let insert_result = self.insert_stream(Box::new(file), options).await?;
        tokio::fs::remove_file(src).await.int_err()?;
        Ok(insert_result)
    }

    #[tracing::instrument(level = "debug", name = ObjectRepositoryS3_delete, skip_all, fields(%hash))]
    async fn delete(&self, hash: &Multihash) -> Result<(), DeleteError> {
        let key = self.get_key(hash);

        tracing::debug!(?key, "Deleting object");

        self.s3_context
            .delete_object(key)
            .await
            // TODO: Detect credentials error
            .map_err(|e| e.into_service_error().int_err())?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
