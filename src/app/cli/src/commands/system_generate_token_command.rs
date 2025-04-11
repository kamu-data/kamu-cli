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

use crate::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct GenerateTokenCommand {
    jwt_token_issuer: Arc<dyn kamu_accounts::JwtTokenIssuer>,

    #[dill::component(explicit)]
    login: Option<String>,

    #[dill::component(explicit)]
    subject: Option<String>,

    #[dill::component(explicit)]
    expiration_time_sec: usize,
}

#[async_trait::async_trait(?Send)]
impl Command for GenerateTokenCommand {
    async fn run(&self) -> Result<(), CLIError> {
        let subject = if let Some(subject) = &self.subject {
            odf::AccountID::from_did_str(subject).int_err()?
        } else if let Some(login) = &self.login {
            odf::AccountID::new_seeded_ed25519(login.as_bytes())
        } else {
            return Err(CLIError::usage_error("Specify --login or --subject"));
        };

        let token = self
            .jwt_token_issuer
            .make_access_token_from_account_id(&subject, self.expiration_time_sec)?;

        println!("{token}");
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
