// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use init_on_startup::InitOnStartup;
use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{
    AccountConfig,
    DidSecretEncryptionConfig,
    PredefinedAccountsConfig,
    Principal,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    CreateAccountUseCaseImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
    UpdateAccountUseCaseImpl,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    DeleteDatasetRebacPropertiesUseCaseImpl,
    RebacDatasetLifecycleMessageConsumer,
    RebacServiceImpl,
    SetDatasetRebacPropertiesUseCaseImpl,
};
use kamu_core::TenancyConfig;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_datasets_services::{DatasetEntryServiceImpl, DatasetEntryWriter};
use messaging_outbox::{
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
    register_message_dispatcher,
};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Accounts, dataset entries, `RebacServiceImpl` and the OSO permission checker over
/// in-memory storage, with the predefined accounts already registered.
/// Suites chain the component they test on [`Self::catalog`].
pub(crate) struct BaseOsoRebacHarness {
    catalog: dill::Catalog,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl BaseOsoRebacHarness {
    /// `register_subject` adds the catalog's current account subject
    pub(crate) async fn new(
        predefined_accounts_config: PredefinedAccountsConfig,
        register_subject: impl FnOnce(&mut dill::CatalogBuilder),
    ) -> Self {
        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            kamu_datasets_inmem::register_dependencies(&mut b);
            register_subject(&mut b);
            b.add::<SystemTimeSourceDefault>()
                .add_value(predefined_accounts_config)
                .add::<PredefinedAccountsRegistrator>()
                .add::<RebacServiceImpl>()
                .add_value(DefaultAccountProperties::default())
                .add_value(DefaultDatasetProperties::default())
                .add::<RebacDatasetLifecycleMessageConsumer>()
                .add::<SetDatasetRebacPropertiesUseCaseImpl>()
                .add::<DeleteDatasetRebacPropertiesUseCaseImpl>()
                .add::<UpdateAccountUseCaseImpl>()
                .add::<CreateAccountUseCaseImpl>()
                .add::<InMemoryRebacRepository>()
                .add_builder(OutboxImmediateImpl::builder(ConsumerFilter::AllConsumers))
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add_value(odf::dataset::MockDatasetStorageUnit::new())
                .bind::<dyn odf::DatasetStorageUnit, odf::dataset::MockDatasetStorageUnit>()
                .add_value(TenancyConfig::MultiTenant)
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<LoginPasswordAuthProvider>();

            kamu_adapter_auth_oso_rebac::register_dependencies(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            b.build()
        };

        catalog
            .get_one::<PredefinedAccountsRegistrator>()
            .unwrap()
            .run_initialization()
            .await
            .unwrap();

        Self {
            dataset_entry_writer: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            catalog,
        }
    }

    /// Plain accounts, with IDs derived from their names
    pub(crate) fn predefined_accounts(account_names: &[&str]) -> PredefinedAccountsConfig {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for account_name in account_names {
            predefined_accounts_config.predefined.push(
                AccountConfig::test_config_from_name_with_id(odf::AccountName::new_unchecked(
                    account_name,
                )),
            );
        }
        predefined_accounts_config
    }

    pub(crate) fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub(crate) fn user(account_name: &str) -> Principal {
        Principal::from(&CurrentAccountSubjectTestHelper::logged(account_name))
    }

    pub(crate) async fn create_private_dataset(
        &self,
        owner: &str,
        name: &str,
    ) -> odf::DatasetHandle {
        let handle = odf::metadata::testing::handle(&owner, &name, odf::DatasetKind::Root);
        self.create_dataset(&handle, odf::DatasetVisibility::Private)
            .await;
        handle
    }

    /// Creates a root dataset owned by the account its alias names
    pub(crate) async fn create_dataset(
        &self,
        handle: &odf::DatasetHandle,
        visibility: odf::DatasetVisibility,
    ) {
        let owner_name = handle.alias.account_name.as_ref().unwrap();
        let owner_id = odf::metadata::testing::account_id(owner_name);

        self.dataset_entry_writer
            .create_entry(
                &handle.id,
                &owner_id,
                owner_name,
                &handle.alias.dataset_name,
                odf::DatasetKind::Root,
            )
            .await
            .unwrap();

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    Utc::now(),
                    handle.id.clone(),
                    owner_id,
                    visibility,
                    handle.alias.dataset_name.clone(),
                ),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
