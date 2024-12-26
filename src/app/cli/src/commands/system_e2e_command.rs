// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu::domain::{DatasetRegistry, DatasetRegistryExt, MetadataChainExt};
use kamu_accounts::{AccountConfig, AccountRepository, PROVIDER_PASSWORD};
use kamu_accounts_services::LoginPasswordAuthProvider;
use opendatafabric as odf;

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemE2ECommand {
    action: String,
    arguments: Vec<String>,
    dataset_ref: Option<odf::DatasetRef>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    account_repo: Arc<dyn AccountRepository>,
    login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
}

impl SystemE2ECommand {
    pub fn new<S>(
        action: S,
        arguments: Vec<String>,
        dataset_ref: Option<odf::DatasetRef>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        account_repo: Arc<dyn AccountRepository>,
        login_password_auth_provider: Arc<LoginPasswordAuthProvider>,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            action: action.into(),
            arguments,
            dataset_ref,
            dataset_registry,
            account_repo,
            login_password_auth_provider,
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for SystemE2ECommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        match self.action.as_str() {
            "get-last-data-block-path" => {
                let Some(dataset_ref) = self.dataset_ref.as_ref() else {
                    return Err(CLIError::usage_error("dataset required"));
                };

                let resolved_dataset = self
                    .dataset_registry
                    .get_dataset_by_ref(dataset_ref)
                    .await?;

                let maybe_physical_hash = resolved_dataset
                    .as_metadata_chain()
                    .last_data_block_with_new_data()
                    .await?
                    .into_event()
                    .and_then(|event| event.new_data)
                    .map(|new_data| new_data.physical_hash);

                let Some(physical_hash) = maybe_physical_hash else {
                    return Err(CLIError::usage_error("DataSlice not found"));
                };

                let internal_url = resolved_dataset
                    .as_data_repo()
                    .get_internal_url(&physical_hash)
                    .await;

                let path =
                    kamu_data_utils::data::local_url::into_local_path(internal_url).int_err()?;

                println!("{}", path.display());
            }
            "account-add" => {
                if self.arguments.is_empty() {
                    return Err("Account names have not been provided".int_err().into());
                };

                for account_name in &self.arguments {
                    eprint!("Add {account_name}... ");

                    let account_config =
                        AccountConfig::from_name(odf::AccountName::new_unchecked(account_name));
                    let account = (&account_config).into();

                    self.account_repo.create_account(&account).await.int_err()?;

                    if account_config.provider == PROVIDER_PASSWORD {
                        self.login_password_auth_provider
                            .save_password(&account.account_name, account_config.get_password())
                            .await?;
                    }

                    eprintln!("{}", console::style("Done").green());
                }
            }
            unexpected_action => panic!("Unexpected action: '{unexpected_action}'"),
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
