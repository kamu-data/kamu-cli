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
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use kamu_accounts::*;
use kamu_auth_rebac::{AccountPropertyName, RebacService};
use odf::AccountID;

use crate::LoginPasswordAuthProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A service that aims to register accounts on a one-time basis
pub struct PredefinedAccountsRegistrator {
    predefined_accounts_config: Arc<PredefinedAccountsConfig>,
    login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
    account_repository: Arc<dyn AccountRepository>,
    rebac_service: Arc<dyn RebacService>,
}

#[component(pub)]
#[interface(dyn InitOnStartup)]
#[meta(InitOnStartupMeta {
    job_name: JOB_KAMU_ACCOUNTS_PREDEFINED_ACCOUNTS_REGISTRATOR,
    depends_on: &[],
    requires_transaction: true,
})]
impl PredefinedAccountsRegistrator {
    pub fn new(
        predefined_accounts_config: Arc<PredefinedAccountsConfig>,
        login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
        account_repository: Arc<dyn AccountRepository>,
        rebac_service: Arc<dyn RebacService>,
    ) -> Self {
        Self {
            predefined_accounts_config,
            login_password_auth_provider,
            account_repository,
            rebac_service,
        }
    }

    async fn set_rebac_properties(
        &self,
        account_id: &AccountID,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        // TODO: Revisit if batch property setting will be implemented
        for (name, value) in [
            AccountPropertyName::is_admin(account_config.is_admin),
            AccountPropertyName::can_provision_accounts(account_config.can_provision_accounts),
        ] {
            self.rebac_service
                .set_account_property(account_id, name, &value)
                .await
                .int_err()?;
        }

        Ok(())
    }

    async fn register_unknown_account(
        &self,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let account = account_config.into();

        self.account_repository
            .create_account(&account)
            .await
            .int_err()?;

        if account_config.provider == PROVIDER_PASSWORD {
            self.login_password_auth_provider
                .save_password(&account.account_name, account_config.get_password())
                .await?;
        }

        Ok(())
    }

    async fn compare_and_update_account(
        &self,
        account: Account,
        account_config: &AccountConfig,
    ) -> Result<(), InternalError> {
        let updated_account = Account {
            registered_at: account.registered_at,
            ..account_config.into()
        };

        if account != updated_account {
            self.account_repository
                .update_account(updated_account)
                .await
                .int_err()?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for PredefinedAccountsRegistrator {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "PredefinedAccountsRegistrator::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        for account_config in &self.predefined_accounts_config.predefined {
            let account_id = account_config.get_id();
            match self.account_repository.get_account_by_id(&account_id).await {
                Ok(account) => {
                    self.compare_and_update_account(account, account_config)
                        .await?;
                }
                Err(GetAccountByIdError::NotFound(_)) => {
                    self.register_unknown_account(account_config).await?;
                }
                Err(GetAccountByIdError::Internal(e)) => return Err(e),
            }
            self.set_rebac_properties(&account_id, account_config)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
