// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use tonic::Status;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Simplified trait that some session managers can delegate authentication to
#[async_trait::async_trait]
pub trait SessionAuth: Send + Sync {
    async fn auth_basic(&self, _username: &str, _password: &str) -> Result<(), Status> {
        Err(Status::unauthenticated("Invalid credentials"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn SessionAuth)]
pub struct SessionAuthBasicPredefined {
    accounts_passwords: HashMap<String, String>,
}

#[async_trait::async_trait]
impl SessionAuth for SessionAuthBasicPredefined {
    async fn auth_basic(&self, username: &str, password: &str) -> Result<(), Status> {
        if let Some(expected_password) = self.accounts_passwords.get(username) {
            if expected_password == password {
                return Ok(());
            }
        }
        Err(Status::unauthenticated("Invalid credentials"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
