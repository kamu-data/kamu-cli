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
    CurrentAccountSubject,
    CurrentAccountSubjectCatalogExt as _,
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
use kamu_adapter_auth_oso_rebac::register_dependencies;
use kamu_auth_rebac::{RebacDatasetRefUnresolvedError, RebacDatasetRegistryFacade};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    DeleteDatasetRebacPropertiesUseCaseImpl,
    RebacDatasetLifecycleMessageConsumer,
    RebacDatasetRegistryFacadeImpl,
    RebacServiceImpl,
    SetDatasetRebacPropertiesUseCaseImpl,
};
use kamu_core::TenancyConfig;
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetPermission,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use kamu_datasets_services::{DatasetEntryServiceImpl, DatasetEntryWriter};
use messaging_outbox::{
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
    register_message_dispatcher,
};
use pretty_assertions::{assert_eq, assert_matches};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_facade_checks_the_given_principal_not_the_current_account() {
    let harness =
        RebacDatasetRegistryFacadeHarness::new(CurrentAccountSubjectTestHelper::logged("owner"))
            .await;
    let owned = harness.create_private_dataset("owner", "owned").await;
    let stranger = RebacDatasetRegistryFacadeHarness::user("stranger");

    assert_matches!(
        harness.resolve_handle_as(&stranger, &owned).await,
        Err(RebacDatasetRefUnresolvedError::Access(_))
    );
    assert_eq!(
        harness.accessible_refs_as(&stranger, &owned).await,
        Vec::<odf::DatasetRef>::new()
    );

    assert_matches!(
        harness.resolve_handle_as(&Principal::Node, &owned).await,
        Ok(handle) if handle == owned
    );
    assert_eq!(
        harness.accessible_refs_as(&Principal::Node, &owned).await,
        vec![owned.as_local_ref()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNTS: [&str; 2] = ["owner", "stranger"];

struct RebacDatasetRegistryFacadeHarness {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl RebacDatasetRegistryFacadeHarness {
    /// `current_account_subject` is the catalog's subject; each call names its
    /// principal on its own
    async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();
        for account_name in ACCOUNTS {
            predefined_accounts_config.predefined.push(
                AccountConfig::test_config_from_name_with_id(odf::AccountName::new_unchecked(
                    account_name,
                )),
            );
        }

        let catalog = {
            let mut b = dill::CatalogBuilder::new();

            kamu_datasets_inmem::register_dependencies(&mut b);
            b.add::<SystemTimeSourceDefault>()
                .add_current_account_subject(current_account_subject)
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
                .add::<LoginPasswordAuthProvider>()
                .add::<RebacDatasetRegistryFacadeImpl>();

            register_dependencies(&mut b);

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
            rebac_dataset_registry_facade: catalog.get_one().unwrap(),
            dataset_entry_writer: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
        }
    }

    async fn create_private_dataset(&self, owner: &str, name: &str) -> odf::DatasetHandle {
        let handle = odf::metadata::testing::handle(&owner, &name, odf::DatasetKind::Root);
        let owner_id = odf::metadata::testing::account_id(&owner);

        self.dataset_entry_writer
            .create_entry(
                &handle.id,
                &owner_id,
                &odf::AccountName::new_unchecked(owner),
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
                    odf::DatasetVisibility::Private,
                    handle.alias.dataset_name.clone(),
                ),
            )
            .await
            .unwrap();

        handle
    }

    async fn resolve_handle_as(
        &self,
        principal: &Principal,
        dataset: &odf::DatasetHandle,
    ) -> Result<odf::DatasetHandle, RebacDatasetRefUnresolvedError> {
        self.rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(
                principal,
                &dataset.as_local_ref(),
                DatasetPermission::Read,
            )
            .await
    }

    /// The refs among `[dataset]` the principal may read
    async fn accessible_refs_as(
        &self,
        principal: &Principal,
        dataset: &odf::DatasetHandle,
    ) -> Vec<odf::DatasetRef> {
        self.rebac_dataset_registry_facade
            .classify_dataset_refs_by_allowance(
                principal,
                &[&dataset.as_local_ref()],
                DatasetPermission::Read,
            )
            .await
            .unwrap()
            .accessible_resolved_refs
            .into_iter()
            .map(|(dataset_ref, _)| dataset_ref)
            .collect()
    }

    fn user(account_name: &str) -> Principal {
        Principal::from(&CurrentAccountSubjectTestHelper::logged(account_name))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
