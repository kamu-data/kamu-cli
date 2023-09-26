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
pub enum RemoteServerCredentialsScope {
    Workspace,
    User,
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RemoteServerCredentials {
    pub server_frontend_url: Url,
    pub server_backend_url: Url,
    by_account_name: HashMap<AccountName, RemoteServerAccountCredentials>,
}

impl RemoteServerCredentials {
    pub fn new(server_frontend_url: Url, server_backend_url: Url) -> Self {
        Self {
            server_frontend_url,
            server_backend_url,
            by_account_name: HashMap::new(),
        }
    }

    pub fn add_account_credentials(
        &mut self,
        account_name: AccountName,
        credentials: RemoteServerAccountCredentials,
    ) {
        self.by_account_name.insert(account_name, credentials);
    }

    pub fn drop_account_credentials(
        &mut self,
        account_name: &AccountName,
    ) -> Option<RemoteServerAccountCredentials> {
        self.by_account_name.remove(account_name)
    }

    pub fn for_account<'a>(
        &'a self,
        account_name: &AccountName,
    ) -> Option<&'a RemoteServerAccountCredentials> {
        self.by_account_name.get(account_name)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum RemoteServerAccountCredentials {
    AccessToken(RemoteServerAccessToken),
    APIKey(RemoteServerAPIKey),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RemoteServerAccessToken {
    pub access_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RemoteServerAPIKey {
    pub api_key: String,
    pub secret_key: String,
}

////////////////////////////////////////////////////////////////////////////////////////
