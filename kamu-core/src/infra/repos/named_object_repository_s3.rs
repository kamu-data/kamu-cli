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
use crate::infra::utils::s3_context::S3Context;

use async_trait::async_trait;
use bytes::Bytes;
use rusoto_core::RusotoError;
use rusoto_s3::*;
use tracing::debug;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryS3 {
    s3_context: S3Context,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryS3 {
    pub fn new(s3_context: S3Context) -> Self {
        Self { s3_context }
    }

    fn get_key(&self, name: &str) -> String {
        self.s3_context.get_key(name)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryS3 {
    async fn get(&self, name: &str) -> Result<Bytes, GetError> {
        let key = self.get_key(name);

        debug!(?key, "Reading object stream");

        let resp = match self
            .s3_context
            .client
            .get_object(GetObjectRequest {
                bucket: self.s3_context.bucket.clone(),
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
            .s3_context
            .client
            .put_object(PutObjectRequest {
                bucket: self.s3_context.bucket.clone(),
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
            .s3_context
            .client
            .delete_object(DeleteObjectRequest {
                bucket: self.s3_context.bucket.clone(),
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
