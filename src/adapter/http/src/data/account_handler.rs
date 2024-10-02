// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use axum::extract::Extension;
use axum::response::Json;
use chrono::{DateTime, Utc};
use dill::Catalog;
use http_common::*;
use kamu_accounts::{
    Account,
    AccountDisplayName,
    AccountType,
    AuthenticationService,
    CurrentAccountSubject,
};
use opendatafabric::{AccountID, AccountName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Response {
    pub id: AccountID,
    pub account_name: AccountName,
    pub email: Option<String>,
    pub display_name: AccountDisplayName,
    pub account_type: AccountType,
    pub avatar_url: Option<String>,
    pub registered_at: DateTime<Utc>,
    // TODO: ReBAC: absorb the `is_admin` attribute from the Accounts domain
    //       https://github.com/kamu-data/kamu-cli/issues/766
    pub is_admin: bool,
    pub provider: String,
    pub provider_identity_key: String,
}

impl From<Account> for Response {
    fn from(value: Account) -> Self {
        Self {
            id: value.id,
            account_name: value.account_name,
            email: value.email,
            display_name: value.display_name,
            account_type: value.account_type,
            avatar_url: value.avatar_url,
            registered_at: value.registered_at,
            is_admin: value.is_admin,
            provider: value.provider,
            provider_identity_key: value.provider_identity_key,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn account_handler(
    Extension(catalog): Extension<Catalog>,
) -> Result<Json<Response>, ApiError> {
    let response = get_account(&catalog).await?;
    tracing::debug!(?response, "Get account info response");
    Ok(response)
}

async fn get_account(catalog: &Catalog) -> Result<Json<Response>, ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Anonymous(_) => Err(ApiError::new_unauthorized()),
        CurrentAccountSubject::Logged(account) => {
            let auth_service = catalog.get_one::<dyn AuthenticationService>().unwrap();
            let full_account_info_maybe = auth_service.account_by_id(&account.account_id).await?;
            if let Some(full_account_info) = full_account_info_maybe {
                return Ok(Json(full_account_info.into()));
            }

            Err(ApiError::not_found_without_body())
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
