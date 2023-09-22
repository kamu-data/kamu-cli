// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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
    pub local_account_name: AccountName,
    pub kind: RemoteServerCredentialsKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RemoteServerCredentialsKind {
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
