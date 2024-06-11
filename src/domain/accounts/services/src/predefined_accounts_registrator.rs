// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use internal_error::*;
use kamu_accounts::*;

use crate::LoginPasswordAuthProvider;

////////////////////////////////////////////////////////////////////////////////

/// A service that aims to register accounts on a one-time basis
pub struct PredefinedAccountsRegistrator {
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    maybe_login_password_auth_provider: Option<Arc<LoginPasswordAuthProvider>>,
    account_repository: Arc<dyn AccountRepository>,
}

#[component(pub)]
impl PredefinedAccountsRegistrator {
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        maybe_login_password_auth_provider: Option<Arc<LoginPasswordAuthProvider>>,
        account_repository: Arc<dyn AccountRepository>,
    ) -> Self {
        Self {
            predefined_accounts_config,
            maybe_login_password_auth_provider,
            account_repository,
        }
    }

    pub async fn ensure_predefined_accounts_are_registered(&self) -> Result<(), InternalError> {
        for account_config in &self.predefined_accounts_config.predefined {
            let account_id = account_config.get_id();
            let is_unknown_account =
                match self.account_repository.get_account_by_id(&account_id).await {
                    Ok(_) => Ok(false),
                    Err(GetAccountByIdError::NotFound(_)) => Ok(true),
                    Err(GetAccountByIdError::Internal(e)) => Err(e),
                }?;

            if is_unknown_account {
                self.register_unknown_account(account_config).await?;
            }
        }

        Ok(())
    }

    async fn register_unknown_account(
        &self,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let account = Account {
            id: account_config.get_id(),
            account_name: account_config.account_name.clone(),
            email: account_config.email.clone(),
            display_name: account_config.get_display_name(),
            account_type: account_config.account_type,
            avatar_url: account_config.avatar_url.clone(),
            registered_at: account_config.registered_at,
            is_admin: account_config.is_admin,
            provider: account_config.provider.clone(),
            provider_identity_key: account_config.account_name.to_string(),
        };

        self.account_repository
            .create_account(&account)
            .await
            .map_err(|e| match e {
                CreateAccountError::Duplicate(e) => e.int_err(),
                CreateAccountError::Internal(e) => e,
            })?;

        if account_config.provider == PROVIDER_PASSWORD
            && let Some(login_password_auth_provider) =
                self.maybe_login_password_auth_provider.as_ref()
        {
            login_password_auth_provider
                .save_password(&account.account_name, account_config.get_password())
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////
