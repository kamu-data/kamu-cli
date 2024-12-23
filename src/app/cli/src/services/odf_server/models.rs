// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone)]
pub enum AccessTokenStoreScope {
    Workspace,
    User,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServerAccessTokensRecord {
    pub frontend_url: Option<Url>,
    pub backend_url: Url,
    tokens: Vec<AccessToken>,
}

impl ServerAccessTokensRecord {
    pub fn new(frontend_url: Option<Url>, backend_url: Url) -> Self {
        Self {
            frontend_url,
            backend_url,
            tokens: Vec::new(),
        }
    }

    pub fn add_account_token(&mut self, access_token: AccessToken) {
        match self.token_position(&access_token.account_name) {
            Some(i) => self.tokens[i] = access_token,
            None => self.tokens.push(access_token),
        }
    }

    pub fn drop_account_token(&mut self, account_name: &odf::AccountName) -> Option<AccessToken> {
        let maybe_position = self.token_position(account_name);
        if let Some(position) = maybe_position {
            Some(self.tokens.remove(position))
        } else {
            None
        }
    }

    pub fn token_for_account(&self, account_name: &odf::AccountName) -> Option<&AccessToken> {
        self.token_position(account_name)
            .map(|i| self.tokens.get(i).unwrap())
    }

    fn token_position(&self, account_name: &odf::AccountName) -> Option<usize> {
        // Note: linear search is fine for CLI scenarios, we don't expect long lists.
        // While storing a vector of records instead of map greatly simplifies
        // serialization
        self.tokens
            .iter()
            .position(|t| &t.account_name == account_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AccessToken {
    pub account_name: odf::AccountName,
    pub access_token: String,
}

impl AccessToken {
    pub fn new(account_name: odf::AccountName, access_token: String) -> Self {
        Self {
            account_name,
            access_token,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct AccessTokenFindReport {
    pub frontend_url: Option<Url>,
    pub backend_url: Url,
    pub access_token: AccessToken,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
