// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::*;
use kamu_accounts_services::AuthenticationServiceImpl;

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct DebugTokenCommand {
    auth_service: Arc<AuthenticationServiceImpl>,

    #[dill::component(explicit)]
    access_token: String,
}

#[async_trait::async_trait(?Send)]
impl Command for DebugTokenCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let token = self
            .auth_service
            .decode_access_token(&self.access_token)
            .int_err()?;

        println!("{token:#?}");
        Ok(())
    }
}
