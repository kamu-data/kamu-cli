// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use aws_sdk_s3::operation::get_object::GetObjectOutput;
use bytes::Bytes;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use odf_storage::*;
use s3_utils::{PutObjectCondition, S3Context};

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

    async fn get_with_e_tag(&self, key: &str) -> Result<Option<(Vec<u8>, String)>, InternalError> {
        let resp = match self.s3_context.get_object(key.to_owned()).await {
            Ok(resp) => resp,
            Err(err) => {
                let err = err.into_service_error();
                return if err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(err.int_err())
                };
            }
        };

        let e_tag = resp
            .e_tag()
            .ok_or_else(|| format!("Object has no ETag: {key}").int_err())?
            .to_owned();

        Ok(Some((Self::read_body(resp).await?, e_tag)))
    }

    async fn read_body(resp: GetObjectOutput) -> Result<Vec<u8>, InternalError> {
        let mut stream = resp.body.into_async_read();

        use tokio::io::AsyncReadExt;
        let mut data: Vec<u8> = Vec::new();
        stream.read_to_end(&mut data).await.int_err()?;

        Ok(data)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PRECONDITION_FAILED: u16 = 412;
const CONFLICT: u16 = 409;
const NOT_IMPLEMENTED: u16 = 501;

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
            Err(err) => {
                let err = err.into_service_error();
                // TODO: Detect credentials error
                if err.is_no_such_key() {
                    Err(GetNamedError::NotFound(NotFoundError {
                        name: name.to_owned(),
                    }))
                } else {
                    Err(err.int_err().into())
                }
            }
        }?;

        Ok(Bytes::from(Self::read_body(resp).await?))
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

    #[tracing::instrument(level = "debug", name = NamedObjectRepositoryS3_compare_and_set, skip_all, fields(%name))]
    async fn compare_and_set(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        data: &[u8],
    ) -> Result<(), CompareAndSetNamedError> {
        let key = self.get_key(name);
        let mismatch = || {
            CompareAndSetNamedError::Mismatch(NamedObjectMismatchError {
                name: name.to_owned(),
            })
        };

        // The write is conditional on the ETag of the content that was compared,
        // so a change after this read fails the write
        let condition = match expected {
            None => PutObjectCondition::IfNoneMatch,
            Some(expected) => {
                let Some((actual, e_tag)) = self.get_with_e_tag(&key).await? else {
                    return Err(mismatch());
                };
                if actual != expected {
                    return Err(mismatch());
                }
                PutObjectCondition::IfMatch(e_tag)
            }
        };

        tracing::debug!(?key, "Inserting object conditionally");

        match self
            .s3_context
            .put_object_conditional(key, data, condition)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => match err.raw_response().map(|r| r.status().as_u16()) {
                Some(PRECONDITION_FAILED | CONFLICT) => Err(mismatch()),
                Some(NOT_IMPLEMENTED) => Err(CompareAndSetNamedError::NotSupported),
                // TODO: Detect credentials error
                _ => Err(err.into_service_error().int_err().into()),
            },
        }
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
