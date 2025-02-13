// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashSet;
use std::sync::Arc;

use dill::Component;
use kamu_accounts::testing::CurrentAccountSubjectTestHelper;
use kamu_accounts::{AccountConfig, CurrentAccountSubject, PredefinedAccountsConfig};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::TenancyConfig;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_services::{DatasetEntryServiceImpl, DatasetEntryWriter};
use messaging_outbox::{
    register_message_dispatcher,
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
};
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_own_private_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(write_result, Ok(()));
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions == HashSet::from([DatasetAction::Read, DatasetAction::Write])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_own_public_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(write_result, Ok(()));
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions == HashSet::from([DatasetAction::Read, DatasetAction::Write])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write_public_dataset() {
    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    let dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions == HashSet::from([DatasetAction::Read])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_not_read_and_write_private_dataset() {
    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    let dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(
        read_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_read_but_not_write_public_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;
    let dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(read_result, Ok(()));
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions == HashSet::from([DatasetAction::Read])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_not_read_and_write_private_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;
    let dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    let read_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Read)
        .await;
    let write_result = harness
        .dataset_authorizer
        .check_action_allowed(&dataset_id, DatasetAction::Write)
        .await;
    let allowed_actions = harness
        .dataset_authorizer
        .get_allowed_actions(&dataset_id)
        .await;

    assert_matches!(
        read_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        write_result,
        Err(DatasetActionUnauthorizedError::Access(
            odf::AccessError::Forbidden(_)
        ))
    );
    assert_matches!(
        allowed_actions,
        Ok(actual_actions)
            if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[allow(dead_code)]
pub struct DatasetAuthorizerHarness {
    dataset_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl DatasetAuthorizerHarness {
    pub async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();

        if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
            predefined_accounts_config
                .predefined
                .push(AccountConfig::test_config_from_name(
                    logged_account.account_name.clone(),
                ));
        }

        let catalog = {
            let tenancy_config = TenancyConfig::MultiTenant;

            let mut b = dill::CatalogBuilder::new();

            b.add::<SystemTimeSourceDefault>()
                .add_value(current_account_subject)
                .add_value(predefined_accounts_config)
                .add::<PredefinedAccountsRegistrator>()
                .add::<kamu_auth_rebac_services::RebacServiceImpl>()
                .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
                .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                    allows_anonymous_read: false,
                    allows_public_read: false,
                })
                .add::<kamu_auth_rebac_services::MultiTenantRebacDatasetLifecycleMessageConsumer>()
                .add::<InMemoryRebacRepository>()
                .add_builder(
                    OutboxImmediateImpl::builder()
                        .with_consumer_filter(ConsumerFilter::AllConsumers),
                )
                .bind::<dyn Outbox, OutboxImmediateImpl>()
                .add_value(odf::dataset::MockDatasetStorageUnit::new())
                .bind::<dyn odf::DatasetStorageUnit, odf::dataset::MockDatasetStorageUnit>()
                .add_value(tenancy_config)
                .add::<DatasetEntryServiceImpl>()
                .add::<InMemoryDatasetEntryRepository>()
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

        {
            use init_on_startup::InitOnStartup;
            catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .unwrap()
                .run_initialization()
                .await
                .unwrap();
        };

        Self {
            dataset_authorizer: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            dataset_entry_writer: catalog.get_one().unwrap(),
        }
    }

    async fn create_public_dataset(&self, alias: odf::DatasetAlias) -> odf::DatasetID {
        self.create_dataset(alias, odf::DatasetVisibility::Public)
            .await
    }

    async fn create_private_dataset(&self, alias: odf::DatasetAlias) -> odf::DatasetID {
        self.create_dataset(alias, odf::DatasetVisibility::Private)
            .await
    }

    async fn create_dataset(
        &self,
        alias: odf::DatasetAlias,
        visibility: odf::DatasetVisibility,
    ) -> odf::DatasetID {
        let dataset_id = dataset_id(&alias);
        let account_id = account_id(&alias);

        self.dataset_entry_writer
            .create_entry(&dataset_id, &account_id, &alias.dataset_name)
            .await
            .unwrap();

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_id.clone(),
                    account_id,
                    visibility,
                    alias.dataset_name.clone(),
                ),
            )
            .await
            .unwrap();

        dataset_id
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn dataset_id(alias: &odf::DatasetAlias) -> odf::DatasetID {
    odf::DatasetID::new_seeded_ed25519(alias.to_string().as_bytes())
}

fn account_id(alias: &odf::DatasetAlias) -> odf::AccountID {
    odf::AccountID::new_seeded_ed25519(alias.account_name.as_ref().unwrap().as_bytes())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
