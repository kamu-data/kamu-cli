// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::Extension;
use dill::Catalog;
use http_common::{ApiError, ApiErrorResponse, ResultIntoApiError};

use crate::axum_utils::ensure_authenticated_account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Validate auth token
#[utoipa::path(
    get,
    path = "/token/validate",
    responses(
        (status = OK, body = ()),
        (status = UNAUTHORIZED, body = ApiErrorResponse)
    ),
    tag = "kamu",
    security(
        ("api_key" = []),
    )
)]
pub async fn token_validate_handler(catalog: Extension<Catalog>) -> Result<(), ApiError> {
    ensure_authenticated_account(&catalog).api_err()?;
    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
