// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::Component;
use kamu::testing::{BaseUseCaseHarness, BaseUseCaseHarnessOptions, MockDatasetActionAuthorizer};
use kamu_accounts::{
    AccountConfig,
    DEFAULT_ACCOUNT_NAME_STR,
    DidSecretEncryptionConfig,
    PredefinedAccountsConfig,
};
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_core::auth::DatasetAction;
use kamu_core::*;
use kamu_datasets::*;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::testing::FakeDatasetEntryService;
use kamu_datasets_services::*;
use messaging_outbox::{
    ConsumerFilter,
    Outbox,
    OutboxExt,
    OutboxImmediateImpl,
    register_message_dispatcher,
};
use time_source::SystemTimeSource;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn test_all_downstream_dependencies_are_accessible() {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    use odf::metadata::testing::handle;

    let alice_root_1_dataset_handle = handle(&alice, &"root-1", odf::DatasetKind::Root);
    let alice_root_2_dataset_handle = handle(&alice, &"root-2", odf::DatasetKind::Root);
    let bob_root_3_dataset_handle = handle(&bob, &"root-3", odf::DatasetKind::Root);
    let alice_derived_4_dataset_handle = handle(&alice, &"derived-4", odf::DatasetKind::Derivative);
    let alice_derived_5_dataset_handle = handle(&alice, &"derived-5", odf::DatasetKind::Derivative);
    let bob_derived_6_dataset_handle = handle(&bob, &"derived-6", odf::DatasetKind::Derivative);
    //   ┌──────────────┐
    //   │ alice/root-1 │
    //   └──────────────┘
    // ┌───────────────────────────────────────────┐
    // │ Test use-case datasets                    │
    // │                       ┌─────────────────┐ │
    // │                    ┌──┤ alice/derived-4 │ │
    // │                    │  └─────────────────┘ │
    // │ ┌──────────────┐   │  ┌─────────────────┐ │
    // │ │ alice/root-2 │◄──┼──┤ alice/derived-5 │ │
    // │ └──────────────┘   │  └─────────────────┘ │
    // │                    │  ┌─────────────────┐ │
    // │                    └──┤  bob/derived-6  │ │
    // │                       └─────────────────┘ │
    // └───────────────────────────────────────────┘
    //   ┌──────────────┐
    //   │  bob/root-3  │
    //   └──────────────┘
    let harness = GetDatasetDownstreamDependenciesUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_dataset_ids_by_allowance(
            DatasetAction::Read,
            1,
            // All
            [
                alice_root_1_dataset_handle.id.clone(),
                alice_root_2_dataset_handle.id.clone(),
                bob_root_3_dataset_handle.id.clone(),
                alice_derived_4_dataset_handle.id.clone(),
                alice_derived_5_dataset_handle.id.clone(),
                bob_derived_6_dataset_handle.id.clone(),
            ]
            .into(),
        ),
        [alice.clone(), bob.clone()],
    )
    .await;

    harness
        .create_root_dataset(alice_root_1_dataset_handle.clone())
        .await;
    harness
        .create_root_dataset(alice_root_2_dataset_handle)
        .await;
    harness.create_root_dataset(bob_root_3_dataset_handle).await;
    harness
        .create_derived_dataset(
            bob_derived_6_dataset_handle.clone(),
            vec![alice_root_1_dataset_handle.id.clone()],
        )
        .await;
    harness
        .create_derived_dataset(
            alice_derived_4_dataset_handle.clone(),
            vec![alice_root_1_dataset_handle.id.clone()],
        )
        .await;
    harness
        .create_derived_dataset(
            alice_derived_5_dataset_handle.clone(),
            vec![alice_root_1_dataset_handle.id.clone()],
        )
        .await;

    let res = harness
        .use_case
        .execute(&alice_root_1_dataset_handle.id.clone())
        .await;

    assert!(res.is_ok(), "{res:?}");

    let mut actual_res = res.unwrap();
    actual_res.sort();

    pretty_assertions::assert_eq!(
        [
            DatasetDependency::resolved(
                alice_derived_4_dataset_handle,
                odf::AccountID::new_seeded_ed25519(alice.as_bytes()),
                alice.clone(),
            ),
            DatasetDependency::resolved(
                alice_derived_5_dataset_handle,
                odf::AccountID::new_seeded_ed25519(alice.as_bytes()),
                alice,
            ),
            DatasetDependency::resolved(
                bob_derived_6_dataset_handle,
                odf::AccountID::new_seeded_ed25519(bob.as_bytes()),
                bob,
            ),
        ],
        *actual_res,
    );
}

#[tokio::test]
async fn test_inaccessible_downstream_dependencies_excluded() {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    use odf::metadata::testing::handle;

    let alice_root_1_dataset_handle = handle(&alice, &"root-1", odf::DatasetKind::Root);
    let alice_root_2_dataset_handle = handle(&alice, &"root-2", odf::DatasetKind::Root);
    let bob_root_3_dataset_handle = handle(&bob, &"root-3", odf::DatasetKind::Root);
    let alice_public_derived_4_dataset_handle =
        handle(&alice, &"public-derived-4", odf::DatasetKind::Derivative);
    let alice_private_derived_5_dataset_handle =
        handle(&alice, &"private-derived-5", odf::DatasetKind::Derivative);
    let bob_private_derived_6_dataset_handle =
        handle(&bob, &"private-derived-6", odf::DatasetKind::Derivative);
    //   ┌──────────────┐
    //   │ alice/root-1 │
    //   └──────────────┘
    // ┌───────────────────────────────────────────────────┐
    // │ Test use-case datasets                            │
    // │ (accessible only "alice/public-derived-1")        │
    // │                       ┌─────────────────────────┐ │
    // │                    ┌──┤  alice/public-derived-4 │ │
    // │                    │  └─────────────────────────┘ │
    // │ ┌──────────────┐   │  ┌─────────────────────────┐ │
    // │ │ alice/root-2 │◄──┼──┤ alice/private-derived-5 │ │
    // │ └──────────────┘   │  └─────────────────────────┘ │
    // │                    │  ┌─────────────────────────┐ │
    // │                    └──┤  bob/private-derived-6  │ │
    // │                       └─────────────────────────┘ │
    // └───────────────────────────────────────────────────┘
    //   ┌──────────────┐
    //   │  bob/root-3  │
    //   └──────────────┘
    let harness = GetDatasetDownstreamDependenciesUseCaseHarness::new(
        MockDatasetActionAuthorizer::new().make_expect_classify_dataset_ids_by_allowance(
            DatasetAction::Read,
            1,
            [
                alice_root_1_dataset_handle.id.clone(),
                alice_root_2_dataset_handle.id.clone(),
                bob_root_3_dataset_handle.id.clone(),
                alice_public_derived_4_dataset_handle.id.clone(),
                // Excluded:
                // alice_private_derived_5_dataset_handle.id.clone(),
                // bob_private_derived_6_dataset_handle.id.clone(),
            ]
            .into(),
        ),
        [alice.clone(), bob.clone()],
    )
    .await;

    harness
        .create_root_dataset(alice_root_1_dataset_handle.clone())
        .await;
    harness
        .create_root_dataset(alice_root_2_dataset_handle.clone())
        .await;
    harness.create_root_dataset(bob_root_3_dataset_handle).await;
    harness
        .create_derived_dataset(
            alice_public_derived_4_dataset_handle.clone(),
            vec![alice_root_2_dataset_handle.id.clone()],
        )
        .await;
    harness
        .create_derived_dataset(
            alice_private_derived_5_dataset_handle.clone(),
            vec![alice_root_2_dataset_handle.id.clone()],
        )
        .await;
    harness
        .create_derived_dataset(
            bob_private_derived_6_dataset_handle.clone(),
            vec![alice_root_2_dataset_handle.id.clone()],
        )
        .await;

    let res = harness
        .use_case
        .execute(&alice_root_2_dataset_handle.id.clone())
        .await;

    assert!(res.is_ok(), "{res:?}");

    let mut actual_res = res.unwrap();
    actual_res.sort();

    pretty_assertions::assert_eq!(
        [DatasetDependency::resolved(
            alice_public_derived_4_dataset_handle,
            odf::AccountID::new_seeded_ed25519(alice.as_bytes()),
            alice.clone(),
        )],
        *actual_res,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseUseCaseHarness, base_use_case_harness)]
struct GetDatasetDownstreamDependenciesUseCaseHarness {
    base_use_case_harness: BaseUseCaseHarness,
    use_case: Arc<dyn GetDatasetDownstreamDependenciesUseCase>,
    outbox: Arc<dyn Outbox>,
    fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl GetDatasetDownstreamDependenciesUseCaseHarness {
    async fn new(
        mock_dataset_action_authorizer: MockDatasetActionAuthorizer,
        predefined_account: impl IntoIterator<Item = odf::AccountName>,
    ) -> Self {
        let base_use_case_harness = BaseUseCaseHarness::new(
            BaseUseCaseHarnessOptions::new()
                .with_tenancy_config(TenancyConfig::MultiTenant)
                .with_maybe_authorizer(Some(mock_dataset_action_authorizer))
                .without_outbox(),
        );

        let mut b = dill::CatalogBuilder::new_chained(base_use_case_harness.catalog());
        b.add_builder(
            OutboxImmediateImpl::builder().with_consumer_filter(ConsumerFilter::AllConsumers),
        )
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<GetDatasetDownstreamDependenciesUseCaseImpl>()
        .add::<PredefinedAccountsRegistrator>()
        .add::<RebacServiceImpl>()
        .add::<InMemoryRebacRepository>()
        .add_value(DefaultAccountProperties::default())
        .add_value(DefaultDatasetProperties::default())
        .add_value(PredefinedAccountsConfig {
            predefined: predefined_account
                .into_iter()
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .add::<LoginPasswordAuthProvider>()
        .add::<FakeDatasetEntryService>()
        .add::<DependencyGraphServiceImpl>()
        .add::<InMemoryDatasetDependencyRepository>()
        .add::<AccountServiceImpl>()
        .add::<InMemoryDidSecretKeyRepository>()
        .add_value(DidSecretEncryptionConfig::sample())
        .add::<InMemoryAccountRepository>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        register_message_dispatcher::<DatasetDependenciesMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
        );

        let catalog = b.build();

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
            base_use_case_harness,
            use_case: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
        }
    }

    pub async fn create_root_dataset(
        &self,
        dataset_handle: odf::DatasetHandle,
    ) -> odf::DatasetHandle {
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_handle.alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_handle.alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.system_time_source.now(),
            id: dataset_handle.id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_handle.alias.dataset_name.clone(),
            kind: odf::DatasetKind::Root,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_handle.id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_handle.alias.dataset_name.clone(),
                ),
            )
            .await
            .unwrap();

        dataset_handle
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_handle: odf::DatasetHandle,
        input_ids: Vec<odf::DatasetID>,
    ) -> odf::DatasetHandle {
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_handle.alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_handle.alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.system_time_source.now(),
            id: dataset_handle.id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_handle.alias.dataset_name.clone(),
            kind: odf::DatasetKind::Derivative,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    dataset_handle.id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_handle.alias.dataset_name.clone(),
                ),
            )
            .await
            .unwrap();

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                DatasetDependenciesMessage::updated(&dataset_handle.id, input_ids, vec![]),
            )
            .await
            .unwrap();

        dataset_handle
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
