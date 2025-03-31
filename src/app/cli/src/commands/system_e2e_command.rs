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
use kamu::domain::{DatasetRegistry, DatasetRegistryExt};
use kamu_accounts::{AccountConfig, AccountRepository, PROVIDER_PASSWORD};
use kamu_accounts_services::LoginPasswordAuthProvider;

use super::{CLIError, Command};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Command)]
pub struct SystemE2ECommand {
    dataset_registry: Arc<dyn DatasetRegistry>,
    account_repo: Arc<dyn AccountRepository>,
    login_password_auth_provider: Arc<LoginPasswordAuthProvider>,

    #[dill::component(explicit)]
    action: String,

    #[dill::component(explicit)]
    arguments: Vec<String>,

    #[dill::component(explicit)]
    dataset_ref: Option<odf::DatasetRef>,
}

#[async_trait::async_trait(?Send)]
impl Command for SystemE2ECommand {
    async fn run(&self) -> Result<(), CLIError> {
        match self.action.as_str() {
            "get-last-data-block-path" => {
                let Some(dataset_ref) = self.dataset_ref.as_ref() else {
                    return Err(CLIError::usage_error("dataset required"));
                };

                let resolved_dataset = self
                    .dataset_registry
                    .get_dataset_by_ref(dataset_ref)
                    .await?;

                use odf::dataset::MetadataChainExt;
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

                let path = odf::utils::data::local_url::into_local_path(internal_url).int_err()?;

                println!("{}", path.display());
            }
            "account-add" => {
                if self.arguments.is_empty() {
                    return Err("Account names have not been provided".int_err().into());
                };

                for account_name in &self.arguments {
                    eprint!("Add {account_name}... ");

                    let account_config = AccountConfig::test_config_from_name(
                        odf::AccountName::new_unchecked(account_name),
                    );
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
