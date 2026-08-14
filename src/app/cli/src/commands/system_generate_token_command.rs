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
    account_service: Arc<dyn kamu_accounts::AccountService>,

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
            let candidate_account_id = odf::AccountID::from_did_str(subject).int_err()?;
            let account = self
                .account_service
                .get_account_by_id(&candidate_account_id)
                .await
                .int_err()?;

            account.id
        } else if let Some(login) = &self.login {
            use std::str::FromStr;

            let account_name = odf::AccountName::from_str(login).int_err()?;
            let account = self
                .account_service
                .get_account_by_name(&account_name)
                .await
                .int_err()?;

            account.id
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
