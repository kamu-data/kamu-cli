// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
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

macro_rules! assert_single_dataset {
    (
        setup:
            $harness: expr,
            dataset_id = $dataset_id: expr,
        expected:
            read_result = $( $expected_read_pattern:pat_param )|+ $( if $expected_read_guard: expr )?,
            write_result = $( $expected_write_pattern:pat_param )|+ $( if $expected_write_guard: expr )?,
            allowed_actions_result = $( $expected_allowed_actions_pattern:pat_param )|+ $( if $expected_allowed_actions_guard: expr )?
    ) => {
        assert_matches!(
            $harness
                .dataset_authorizer
                .check_action_allowed(&$dataset_id, DatasetAction::Read)
                .await,
            $( $expected_read_pattern )|+ $( if $expected_read_guard )?
        );
        assert_matches!(
            $harness
                .dataset_authorizer
                .check_action_allowed(&$dataset_id, DatasetAction::Write)
                .await,
            $( $expected_write_pattern )|+ $( if $expected_write_guard )?
        );
        assert_matches!(
            $harness
                .dataset_authorizer
                .get_allowed_actions(&$dataset_id)
                .await,
            $( $expected_allowed_actions_pattern )|+ $( if $expected_allowed_actions_guard )?
        );
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_private_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;

    let owned_private_dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = owned_private_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_owner_can_read_and_write_owned_public_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;

    let owned_public_dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = owned_public_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_read_but_not_write_public_dataset() {
    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;

    let public_dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = public_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_guest_can_not_read_and_write_private_dataset() {
    let harness = DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;

    let private_dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = private_dataset_id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_read_but_not_write_public_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;

    let not_owned_public_dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_public_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_not_owner_can_not_read_and_write_private_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged("not-owner")).await;

    let not_owned_private_dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_id,
        expected:
            read_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            write_result = Err(DatasetActionUnauthorizedError::Access(odf::AccessError::Forbidden(_))),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions.is_empty()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_public_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged_admin()).await;

    let not_owned_public_dataset_id = harness
        .create_public_dataset(odf::metadata::testing::alias(&"owner", &"public-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_public_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_admin_can_read_and_write_not_owned_private_dataset() {
    let harness =
        DatasetAuthorizerHarness::new(CurrentAccountSubjectTestHelper::logged_admin()).await;

    let not_owned_private_dataset_id = harness
        .create_private_dataset(odf::metadata::testing::alias(&"owner", &"private-dataset"))
        .await;

    assert_single_dataset!(
        setup:
            harness,
            dataset_id = not_owned_private_dataset_id,
        expected:
            read_result = Ok(()),
            write_result = Ok(()),
            allowed_actions_result = Ok(actual_actions)
                if actual_actions == [DatasetAction::Read, DatasetAction::Write].into()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetAuthorizerHarness {
    dataset_authorizer: Arc<dyn DatasetActionAuthorizer>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl DatasetAuthorizerHarness {
    pub async fn new(current_account_subject: CurrentAccountSubject) -> Self {
        let mut predefined_accounts_config = PredefinedAccountsConfig::new();

        if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
            let mut account_config =
                AccountConfig::test_config_from_name(logged_account.account_name.clone());
            account_config.is_admin = logged_account.is_admin;

            predefined_accounts_config.predefined.push(account_config);
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
