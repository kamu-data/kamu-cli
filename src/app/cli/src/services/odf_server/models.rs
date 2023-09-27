// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use opendatafabric::AccountName;
use serde::{Deserialize, Serialize};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum AccessTokenStoreScope {
    Workspace,
    User,
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccessTokenMap {
    pub frontend_url: Url,
    pub backend_url: Url,
    tokens_by_account_name: HashMap<AccountName, AccessToken>,
}

impl AccessTokenMap {
    pub fn new(frontend_url: Url, backend_url: Url) -> Self {
        Self {
            frontend_url,
            backend_url,
            tokens_by_account_name: HashMap::new(),
        }
    }

    pub fn add_account_token(&mut self, account_name: AccountName, access_token: AccessToken) {
        self.tokens_by_account_name
            .insert(account_name, access_token);
    }

    pub fn drop_account_token(&mut self, account_name: &AccountName) -> Option<AccessToken> {
        self.tokens_by_account_name.remove(account_name)
    }

    pub fn token_for_account<'a>(&'a self, account_name: &AccountName) -> Option<&'a AccessToken> {
        self.tokens_by_account_name.get(account_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccessToken {
    pub access_token: String,
}

impl AccessToken {
    pub fn new(access_token: String) -> Self {
        Self { access_token }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct AccessTokenFindReport {
    pub frontend_url: Url,
    pub backend_url: Url,
    pub access_token: AccessToken,
}

////////////////////////////////////////////////////////////////////////////////////////
