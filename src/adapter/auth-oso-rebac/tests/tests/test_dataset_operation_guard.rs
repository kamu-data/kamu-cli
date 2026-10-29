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
    DidSecretEncryptionConfig,
    PredefinedAccountsConfig,
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
use kamu_auth_rebac::{AccountToDatasetRelation, RebacService};
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
use kamu_datasets::{
    DatasetLifecycleMessage,
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetPermissionMissingError,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use kamu_datasets_inmem::InMemoryDatasetEntryRepository;
use kamu_datasets_services::{
    DatasetEntryServiceImpl,
    DatasetEntryWriter,
    DatasetOperationGuardImpl,
};
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
async fn test_target_permission_of_each_operation_by_role() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    harness
        .grant("reader", AccountToDatasetRelation::Reader, &target)
        .await;
    harness
        .grant("editor", AccountToDatasetRelation::Editor, &target)
        .await;
    harness
        .grant("maintainer", AccountToDatasetRelation::Maintainer, &target)
        .await;

    // Operation, then whether a Reader, an Editor and a Maintainer may run it
    let expectations = [
        (
            DatasetOperation::Transform { inputs: vec![] },
            [false, true, true],
        ),
        (DatasetOperation::PollingIngest, [false, true, true]),
        (DatasetOperation::PushIngest, [false, true, true]),
        (DatasetOperation::SyncFrom, [false, true, true]),
        (DatasetOperation::SetWatermark, [false, true, true]),
        (DatasetOperation::SyncTo, [true, true, true]),
        (
            DatasetOperation::Verify {
                replayed_inputs: vec![],
            },
            [true, true, true],
        ),
        (DatasetOperation::Compaction, [false, false, true]),
        (DatasetOperation::ResetToMetadata, [false, false, true]),
        (DatasetOperation::Reset, [false, false, true]),
    ];

    for (operation, allowed_by_role) in expectations {
        for (principal, expected_allowed) in ["reader", "editor", "maintainer"]
            .into_iter()
            .zip(allowed_by_role)
        {
            let allowed = harness
                .authorize(principal, &target, operation.clone())
                .await
                .is_ok();
            assert_eq!(
                expected_allowed, allowed,
                "{principal} running {operation:?}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unreadable_input_is_denied_naming_the_input() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    let readable = harness.create_private_dataset("owner", "readable").await;
    let unreadable = harness
        .create_private_dataset("stranger", "unreadable")
        .await;

    for operation in [
        DatasetOperation::Transform {
            inputs: vec![readable.clone(), unreadable.clone()],
        },
        DatasetOperation::Verify {
            replayed_inputs: vec![readable.clone(), unreadable.clone()],
        },
    ] {
        let result = harness.authorize("owner", &target, operation).await;

        assert_eq!(
            Some(unreadable.as_local_ref()),
            OperationGuardHarness::denied_dataset(&result)
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_target_error_takes_precedence_over_input_error() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let target = harness.create_private_dataset("owner", "target").await;
    let input = harness.create_private_dataset("owner", "input").await;
    harness
        .grant("reader", AccountToDatasetRelation::Reader, &target)
        .await;

    let result = harness
        .authorize(
            "reader",
            &target,
            DatasetOperation::Transform {
                inputs: vec![input],
            },
        )
        .await;

    assert_eq!(
        Some(target.as_local_ref()),
        OperationGuardHarness::denied_dataset(&result)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_missing_target_is_not_found() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let missing = odf::metadata::testing::handle(&"owner", &"missing", odf::DatasetKind::Root);

    assert_matches!(
        harness
            .authorize("owner", &missing, DatasetOperation::PollingIngest)
            .await,
        Err(DatasetPermissionError::NotFound(e))
            if e.dataset_ref == missing.as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_principal_is_the_given_one_not_the_current_account() {
    let owner_harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let owned = owner_harness.create_private_dataset("owner", "owned").await;

    assert_matches!(
        owner_harness
            .authorize("stranger", &owned, DatasetOperation::Compaction)
            .await,
        Err(DatasetPermissionError::Access(_))
    );

    let anonymous_harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::anonymous()).await;
    let owned = anonymous_harness
        .create_private_dataset("owner", "owned")
        .await;

    assert_matches!(
        anonymous_harness
            .authorize("owner", &owned, DatasetOperation::Compaction)
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_batch_keeps_request_order() {
    let harness =
        OperationGuardHarness::new(CurrentAccountSubjectTestHelper::logged("owner")).await;
    let owned = harness.create_private_dataset("owner", "owned").await;
    let foreign = harness.create_private_dataset("stranger", "foreign").await;
    let missing = odf::metadata::testing::handle(&"owner", &"missing", odf::DatasetKind::Root);

    let results = harness
        .authorize_all(
            "owner",
            vec![
                DatasetOperationRequest::new(foreign.clone(), DatasetOperation::SyncTo),
                DatasetOperationRequest::new(
                    owned.clone(),
                    DatasetOperation::Transform {
                        inputs: vec![owned.clone()],
                    },
                ),
                DatasetOperationRequest::new(missing.clone(), DatasetOperation::SyncFrom),
                DatasetOperationRequest::new(owned.clone(), DatasetOperation::Reset),
            ],
        )
        .await;

    assert_matches!(
        results.as_slice(),
        [
            Err(DatasetPermissionError::Access(_)),
            Ok(()),
            Err(DatasetPermissionError::NotFound(_)),
            Ok(()),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNTS: [&str; 5] = ["owner", "reader", "editor", "maintainer", "stranger"];

struct OperationGuardHarness {
    operation_guard: Arc<dyn DatasetOperationGuard>,
    rebac_service: Arc<dyn RebacService>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

impl OperationGuardHarness {
    /// `current_account_subject` is the catalog's subject; each check names
    /// its principal on its own
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

            b.add::<SystemTimeSourceDefault>()
                .add_value(current_account_subject)
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
                .add::<InMemoryDatasetEntryRepository>()
                .add::<InMemoryDidSecretKeyRepository>()
                .add_value(DidSecretEncryptionConfig::sample())
                .add::<AccountServiceImpl>()
                .add::<InMemoryAccountRepository>()
                .add::<LoginPasswordAuthProvider>()
                .add::<DatasetOperationGuardImpl>();

            register_dependencies(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );

            b.build()
        };

        {
            catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .unwrap()
                .run_initialization()
                .await
                .unwrap();
        };

        Self {
            operation_guard: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
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

    async fn grant(
        &self,
        account_name: &str,
        relation: AccountToDatasetRelation,
        dataset: &odf::DatasetHandle,
    ) {
        self.rebac_service
            .set_account_dataset_relation(
                &odf::metadata::testing::account_id(&account_name),
                relation,
                &dataset.id,
            )
            .await
            .unwrap();
    }

    async fn authorize(
        &self,
        principal: &str,
        target: &odf::DatasetHandle,
        operation: DatasetOperation,
    ) -> Result<(), DatasetPermissionError> {
        self.operation_guard
            .check_operation(
                &CurrentAccountSubjectTestHelper::logged(principal),
                &DatasetOperationRequest::new(target.clone(), operation),
            )
            .await
    }

    async fn authorize_all(
        &self,
        principal: &str,
        requests: Vec<DatasetOperationRequest>,
    ) -> Vec<Result<(), DatasetPermissionError>> {
        self.operation_guard
            .check_operations(
                &CurrentAccountSubjectTestHelper::logged(principal),
                &requests,
            )
            .await
            .unwrap()
    }

    /// The dataset a permission denial names
    fn denied_dataset(result: &Result<(), DatasetPermissionError>) -> Option<odf::DatasetRef> {
        let Err(DatasetPermissionError::Access(odf::AccessError::Unauthorized(e))) = result else {
            return None;
        };
        e.downcast_ref::<DatasetPermissionMissingError>()
            .map(|e| e.dataset_ref.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
