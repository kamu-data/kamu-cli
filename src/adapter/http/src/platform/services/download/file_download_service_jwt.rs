// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use file_utils::MediaType;
use internal_error::{InternalError, ResultIntoInternal};
use jsonwebtoken::errors::ErrorKind;
use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use kamu_accounts::JwtAuthenticationConfig;
use kamu_core::ServerUrlConfig;
use kamu_core::services::download_service::*;
use serde::{Deserialize, Serialize};
use time_source::SystemTimeSource;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Differs from the access token issuer, so neither token is accepted in place
// of the other
const DOWNLOAD_TOKEN_ISSUER: &str = "dev.kamu.file-download";
const DOWNLOAD_TOKEN_ALGORITHM: Algorithm = Algorithm::HS384;
const DEFAULT_EXPIRES_IN: chrono::Duration = chrono::Duration::hours(1);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FileDownloadService)]
pub struct FileDownloadServiceJwt {
    server_url_config: Arc<ServerUrlConfig>,
    time_source: Arc<dyn SystemTimeSource>,
    jwt_config: Arc<JwtAuthenticationConfig>,
}

impl FileDownloadServiceJwt {
    fn jwt_secret(&self) -> &[u8] {
        self.jwt_config.jwt_secret.as_bytes()
    }
}

impl FileDownloadService for FileDownloadServiceJwt {
    fn make_download_url(
        &self,
        file: FileDownload,
        opts: odf::storage::ExternalTransferOpts,
    ) -> Result<odf::storage::GetExternalUrlResult, InternalError> {
        let issued_at = self.time_source.now();
        let expires_at = issued_at + opts.expiration.unwrap_or(DEFAULT_EXPIRES_IN);

        let claims = FileDownloadClaims {
            iss: DOWNLOAD_TOKEN_ISSUER.to_string(),
            iat: u64::try_from(issued_at.timestamp()).int_err()?,
            exp: u64::try_from(expires_at.timestamp()).int_err()?,
            dataset_id: file.dataset_id.to_string(),
            content_hash: file.content_hash.to_string(),
            content_type: file.content_type.0,
            content_length: file.content_length,
        };

        let token = encode(
            &Header::new(DOWNLOAD_TOKEN_ALGORITHM),
            &claims,
            &EncodingKey::from_secret(self.jwt_secret()),
        )
        .int_err()?;

        let url = Url::parse(&format!(
            "{}platform/file/download/{token}",
            self.server_url_config.protocols.base_url_rest,
        ))
        .int_err()?;

        Ok(odf::storage::GetExternalUrlResult {
            url,
            header_map: http::HeaderMap::new(),
            expires_at: Some(expires_at),
        })
    }

    fn decode_download_token(&self, token: &str) -> Result<FileDownload, DecodeDownloadTokenError> {
        let mut validation = Validation::new(DOWNLOAD_TOKEN_ALGORITHM);
        validation.set_issuer(&[DOWNLOAD_TOKEN_ISSUER]);
        validation.set_required_spec_claims(&["exp", "iss"]);

        let claims = decode::<FileDownloadClaims>(
            token,
            &DecodingKey::from_secret(self.jwt_secret()),
            &validation,
        )
        .map_err(|e| {
            if matches!(e.kind(), ErrorKind::ExpiredSignature) {
                DecodeDownloadTokenError::Expired(ExpiredDownloadTokenError {})
            } else {
                invalid_token(e)
            }
        })?
        .claims;

        Ok(FileDownload {
            dataset_id: odf::DatasetID::from_did_str(&claims.dataset_id).map_err(invalid_token)?,
            content_hash: odf::Multihash::from_multibase(&claims.content_hash)
                .map_err(invalid_token)?,
            content_type: MediaType(claims.content_type),
            content_length: claims.content_length,
        })
    }
}

fn invalid_token<E>(_: E) -> DecodeDownloadTokenError {
    DecodeDownloadTokenError::Invalid(InvalidDownloadTokenError {})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Serialize, Deserialize)]
struct FileDownloadClaims {
    iss: String,
    iat: u64,
    exp: u64,
    dataset_id: String,
    content_hash: String,
    content_type: String,
    content_length: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
