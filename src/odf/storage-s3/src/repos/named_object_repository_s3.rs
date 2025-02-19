// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectError;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use odf_storage::*;
use s3_utils::S3Context;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct NamedObjectRepositoryS3 {
    s3_context: S3Context,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl NamedObjectRepositoryS3 {
    pub fn new(s3_context: S3Context) -> Self {
        Self { s3_context }
    }

    fn get_key(&self, name: &str) -> String {
        self.s3_context.get_key(name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait]
impl NamedObjectRepository for NamedObjectRepositoryS3 {
    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryS3_get, skip_all, fields(%name))]
    async fn get(&self, name: &str) -> Result<Bytes, GetNamedError> {
        let key = self.get_key(name);

        tracing::debug!(?key, "Reading object stream");

        let resp = match self.s3_context.get_object(key).await {
            Ok(resp) => Ok(resp),
            Err(err) => match err.into_service_error() {
                // TODO: Detect credentials error
                GetObjectError::NoSuchKey(_) => Err(GetNamedError::NotFound(NotFoundError {
                    name: name.to_owned(),
                })),
                err => Err(err.int_err().into()),
            },
        }?;

        let mut stream = resp.body.into_async_read();

        use tokio::io::AsyncReadExt;
        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.int_err()?;

        Ok(Bytes::from(data))
    }

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryS3_set, skip_all, fields(%name))]
    async fn set(&self, name: &str, data: &[u8]) -> Result<(), SetNamedError> {
        let key = self.get_key(name);

        tracing::debug!(?key, "Inserting object");

        self.s3_context
            .put_object(key, data)
            .await
            // TODO: Detect credentials error
            .map_err(|e| e.into_service_error().int_err())?;

        Ok(())
    }

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryS3_delete, skip_all, fields(%name))]
    async fn delete(&self, name: &str) -> Result<(), DeleteNamedError> {
        let key = self.get_key(name);

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
