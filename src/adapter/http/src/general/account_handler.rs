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
use database_common_macros::transactional_handler;
use dill::Catalog;
use http_common::*;
use kamu_accounts::{Account, AuthenticationService, CurrentAccountSubject};
use opendatafabric::{AccountID, AccountName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountResponse {
    pub id: AccountID,
    pub account_name: AccountName,
}

impl From<Account> for AccountResponse {
    fn from(value: Account) -> Self {
        Self {
            id: value.id,
            account_name: value.account_name,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[transactional_handler]
pub async fn account_handler(
    Extension(catalog): Extension<Catalog>,
) -> Result<Json<AccountResponse>, ApiError> {
    let response = get_account(&catalog).await?;
    tracing::debug!(?response, "Get account info response");
    Ok(response)
}

async fn get_account(catalog: &Catalog) -> Result<Json<AccountResponse>, ApiError> {
    let current_account_subject = catalog.get_one::<CurrentAccountSubject>().unwrap();
    match current_account_subject.as_ref() {
        CurrentAccountSubject::Anonymous(_) => Err(ApiError::new_unauthorized()),
        CurrentAccountSubject::Logged(account) => {
            let auth_service = catalog.get_one::<dyn AuthenticationService>().unwrap();
            let full_account_info = auth_service
                .account_by_id(&account.account_id)
                .await?
                .unwrap();
            Ok(Json(full_account_info.into()))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
