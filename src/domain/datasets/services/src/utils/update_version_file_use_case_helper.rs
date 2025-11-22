// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{UploadTokenBase64Json, UploadTokenBase64JsonDecodeError};
use kamu_datasets::ContentArgs;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
pub struct UpdateVersionFileUseCaseHelper {
    upload_service: Arc<dyn kamu_core::UploadService>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl UpdateVersionFileUseCaseHelper {
    pub async fn get_content_args<'a>(
        &'a self,
        content_source: ContentSource<'a>,
        content_type: Option<file_utils::MediaType>,
    ) -> Result<ContentArgs, GetContentArgsError> {
        use std::io::Cursor;

        use sha3::Digest;
        use tokio::io::{AsyncReadExt, BufReader};

        match content_source {
            ContentSource::Bytes(bytes) => {
                let reader = BufReader::new(Cursor::new(bytes.to_owned()));

                Ok(ContentArgs {
                    content_length: bytes.len(),
                    content_stream: Some(Box::new(reader)),
                    content_hash: odf::Multihash::from_digest_sha3_256(bytes),
                    content_type,
                })
            }
            ContentSource::Token(token) => {
                let upload_token: UploadTokenBase64Json = token.parse()?;

                let mut stream = self
                    .upload_service
                    .upload_token_into_stream(&upload_token.0)
                    .await
                    .int_err()?;

                let mut digest = sha3::Sha3_256::new();
                let mut buf = [0u8; 2048];

                loop {
                    let read = stream.read(&mut buf).await.int_err()?;
                    if read == 0 {
                        break;
                    }
                    digest.update(&buf[..read]);
                }

                let content_hash =
                    odf::Multihash::new(odf::metadata::Multicodec::Sha3_256, &digest.finalize())
                        .unwrap();

                // Get the stream again and copy data from uploads to storage using computed
                // hash
                // TODO: PERF: Should we create file in the final storage directly to avoid
                // copying?
                let content_stream = self
                    .upload_service
                    .upload_token_into_stream(&upload_token.0)
                    .await
                    .int_err()?;

                Ok(ContentArgs {
                    content_length: upload_token.0.content_length,
                    content_hash,
                    content_stream: Some(content_stream),
                    content_type: upload_token.0.content_type,
                })
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ContentSource<'a> {
    Bytes(&'a [u8]),
    Token(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetContentArgsError {
    #[error(transparent)]
    TokenDecode(#[from] UploadTokenBase64JsonDecodeError),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
