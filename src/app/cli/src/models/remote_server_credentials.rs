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
pub struct RemoteServerCredentials {
    pub server: Url,
    pub account_credentials: HashMap<AccountName, RemoteServerAccountCredentials>,
}

impl RemoteServerCredentials {
    pub fn new(server: Url) -> Self {
        Self {
            server,
            account_credentials: HashMap::new(),
        }
    }

    pub fn add_account_credentials(
        &mut self,
        account_name: AccountName,
        credentials: RemoteServerAccountCredentials,
    ) {
        self.account_credentials.insert(account_name, credentials);
    }

    pub fn for_account(
        &self,
        account_name: &AccountName,
    ) -> Option<RemoteServerAccountCredentials> {
        self.account_credentials
            .get(account_name)
            .map(|c| c.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteServerAccountCredentials {
    AccessToken(RemoteServerAccessToken),
    APIKey(RemoteServerAPIKey),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteServerAccessToken {
    pub access_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteServerAPIKey {
    pub api_key: String,
    pub secret_key: String,
}

////////////////////////////////////////////////////////////////////////////////////////
