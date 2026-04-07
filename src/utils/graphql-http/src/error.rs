// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GraphqlHttpRequestError {
    #[error("Remote GraphQL request to '{endpoint_url}' failed: {message}")]
    Transport { endpoint_url: Url, message: String },

    #[error("Remote GraphQL request to '{endpoint_url}' failed: {status_code} {reason}")]
    HttpStatus {
        endpoint_url: Url,
        status_code: u16,
        reason: String,
    },

    #[error("Remote GraphQL request to '{endpoint_url}' failed: {message}")]
    Graphql { endpoint_url: Url, message: String },

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl GraphqlHttpRequestError {
    pub fn transport(endpoint_url: Url, message: String) -> Self {
        Self::Transport {
            endpoint_url,
            message,
        }
    }

    pub fn http_status(endpoint_url: Url, status: http::StatusCode) -> Self {
        Self::HttpStatus {
            endpoint_url,
            status_code: status.as_u16(),
            reason: status
                .canonical_reason()
                .unwrap_or("Unknown status")
                .to_string(),
        }
    }

    pub fn graphql(endpoint_url: Url, message: String) -> Self {
        Self::Graphql {
            endpoint_url,
            message,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
