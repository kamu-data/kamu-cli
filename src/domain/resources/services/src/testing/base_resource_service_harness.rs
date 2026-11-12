// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use database_common::{InMemoryTransactionManager, NoOpDatabasePlugin};
use dill::CatalogBuilder;
use kamu_accounts::{Account, AccountRepository, AccountType};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_resources::{
    GenericResourceQueryService,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconciliationError,
    ResourceControllerRuntimeConfig,
    ResourceHeadersInput,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourcePhase,
    ResourceRepository,
    ResourceSnapshot,
    ResourceStatus,
    TypeUri,
    reconciliation_error_type_ref,
};
use messaging_outbox::{MockOutbox, Outbox, OutboxProvider, register_message_dispatcher};
use time_source::{FakeSystemTimeSource, SystemTimeSource, SystemTimeSourceStub};
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

use crate::{BackgroundWorkFinalizer, ResourceControllerRuntimeImpl};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Minimal harness providing the resource service infrastructure layer with
/// in-memory storage. Contains no concrete resource types — suitable as a base
/// for any harness that exercises the resource service layer.
pub struct BaseResourceServiceHarness {
    catalog: dill::Catalog,
    generic_query_svc: Arc<dyn GenericResourceQueryService>,
    resource_repo: Arc<dyn ResourceRepository>,
    outbox: Arc<dyn Outbox>,
    time_source: Arc<dyn SystemTimeSource>,
    dispatching_outbox: bool,
}

pub struct BaseResourceServiceHarnessOpts {
    pub outbox_provider: OutboxProvider,
    /// Runs transactions on `InMemoryTransactionManager`, so a rolled-back
    /// transaction leaves no resource, event or outbox message behind. Needs
    /// `OutboxProvider::Dispatching`, whose messages wait in the outbox
    pub transactional: bool,
    /// Time stands still until `FakeSystemTimeSource::advance`, and sleeps on
    /// it end only then. Otherwise `SystemTimeSourceStub`, whose sleeps are real.
    pub fake_time: bool,
    /// Registers the controller runtime, which `settle` drains. The runtime
    /// drives the controllers of the catalog it is registered in, so a harness
    /// that registers types in a chained catalog registers the runtime there
    /// instead, with
    /// [`BaseResourceServiceHarness::register_controller_runtime`], and
    /// finalizes that catalog with [`BackgroundWorkFinalizer`]
    pub controller_runtime: bool,
}

impl Default for BaseResourceServiceHarnessOpts {
    fn default() -> Self {
        Self {
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
            transactional: false,
            fake_time: false,
            controller_runtime: true,
        }
    }
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self {
        Self::new_with_opts(BaseResourceServiceHarnessOpts::default())
    }

    pub fn new_with_opts(opts: BaseResourceServiceHarnessOpts) -> Self {
        Self::new_with_opts_and_additional_dependencies(opts, |_| {})
    }

    pub fn new_with_additional_dependencies(
        register_additional_dependencies: impl FnOnce(&mut CatalogBuilder),
    ) -> Self {
        Self::new_with_opts_and_additional_dependencies(
            BaseResourceServiceHarnessOpts::default(),
            register_additional_dependencies,
        )
    }

    pub fn new_with_opts_and_additional_dependencies(
        opts: BaseResourceServiceHarnessOpts,
        register_additional_dependencies: impl FnOnce(&mut CatalogBuilder),
    ) -> Self {
        let mut b = CatalogBuilder::new();

        if opts.fake_time {
            b.add_value(FakeSystemTimeSource::new(Utc::now()))
                .bind::<dyn SystemTimeSource, FakeSystemTimeSource>();
        } else {
            b.add_value(SystemTimeSourceStub::new_set(Utc::now()))
                .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
        }

        let needs_bridge = matches!(opts.outbox_provider, OutboxProvider::Dispatching);
        opts.outbox_provider.embed_into_catalog(&mut b);

        // The controller hint hub needs it with any outbox
        b.add::<WakeupListenerMetrics>();
        if needs_bridge {
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge>();
            b.add::<kamu_wakeup_listener_inmem::InMemoryWakeupHub>();
        }

        if !needs_bridge {
            // The controller runtime needs it; the dispatching outbox brings
            // its own
            b.add_value(WakeupListenerConfig::local_default());
        }
        if opts.controller_runtime {
            Self::register_controller_runtime(&mut b);
        }

        kamu_resources_inmem::register_dependencies(&mut b);
        b.add::<InMemoryAccountRepository>();

        if opts.transactional {
            assert!(
                needs_bridge,
                "an immediate outbox delivers messages before the transaction ends"
            );
            b.add::<InMemoryTransactionManager>();
        } else {
            NoOpDatabasePlugin::init_database_components(&mut b);
        }

        crate::register_dependencies(&mut b);
        register_additional_dependencies(&mut b);

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();

        let generic_query_svc = catalog.get_one().unwrap();
        let resource_repo = catalog.get_one().unwrap();

        let outbox = catalog.get_one().unwrap();
        let time_source = catalog.get_one().unwrap();

        Self {
            catalog,
            generic_query_svc,
            resource_repo,
            outbox,
            time_source,
            dispatching_outbox: needs_bridge,
        }
    }

    pub fn register_controller_runtime(b: &mut CatalogBuilder) {
        b.add::<ResourceControllerRuntimeImpl>();
        b.add_value(ResourceControllerRuntimeConfig::default());
    }

    /// See [`BackgroundWorkFinalizer`]
    pub async fn settle(&self) {
        self.background_work_finalizer()
            .finalize(&self.catalog)
            .await
            .unwrap();
    }

    /// The finalizer for the outbox of this harness
    pub fn background_work_finalizer(&self) -> BackgroundWorkFinalizer {
        if self.dispatching_outbox {
            BackgroundWorkFinalizer::for_dispatching_outbox()
        } else {
            BackgroundWorkFinalizer::for_immediate_outbox()
        }
    }

    pub fn catalog(&self) -> &dill::Catalog {
        &self.catalog
    }

    pub fn generic_query_svc(&self) -> &dyn GenericResourceQueryService {
        self.generic_query_svc.as_ref()
    }

    pub fn outbox(&self) -> &dyn Outbox {
        self.outbox.as_ref()
    }

    pub fn time_source(&self) -> &dyn SystemTimeSource {
        self.time_source.as_ref()
    }

    pub fn resource_repo(&self) -> &dyn ResourceRepository {
        self.resource_repo.as_ref()
    }

    pub async fn allocate_resource_id(&self) -> ResourceID {
        self.generic_query_svc.allocate_id().await.unwrap()
    }

    /// Registers the given test account handle (see [`test_account_handle`])
    /// in this harness's in-memory `AccountRepository`, so live lookups
    /// (e.g. `InMemoryResourceRepository`'s per-read account-name resolution)
    /// find a match instead of falling back to the deleted-account sentinel.
    /// Idempotent: registering the same handle twice is a no-op.
    pub async fn register_test_account(&self, account: &odf::ResourceOwnerHandle) {
        let account_repository = self.catalog.get_one::<dyn AccountRepository>().unwrap();

        if account_repository
            .get_account_by_id(&account.did)
            .await
            .is_ok()
        {
            return;
        }

        let name = account.name.as_str();
        account_repository
            .save_account(&Account {
                id: account.did.clone(),
                resource_id: account.id,
                account_name: account.name.clone(),
                email: email_utils::Email::parse(&format!("{name}@example.com")).unwrap(),
                display_name: name.to_string(),
                account_type: AccountType::User,
                avatar_url: None,
                registered_at: Utc::now(),
                provider: "unit-test-provider".to_string(),
                provider_identity_key: name.to_string(),
            })
            .await
            .unwrap();
    }

    /// Convenience: derives a handle from `account_name` (see
    /// [`test_account_handle`]) and registers it in one step.
    pub async fn ensure_test_account(&self, account_name: &str) -> odf::ResourceOwnerHandle {
        let handle = odf::ResourceOwnerHandle::new_test(account_name);
        self.register_test_account(&handle).await;
        handle
    }

    /// Builds headers input for an already-resolved account handle (see
    /// [`Self::ensure_test_account`]). `account_id`/`account_name` must
    /// belong together — they are not independently derivable from one
    /// another.
    pub fn make_headers_input(
        account: odf::ResourceOwnerHandle,
        name: &str,
    ) -> ResourceHeadersInput {
        ResourceHeadersInput {
            id: None,
            account: Some(odf::metadata::auth::ResourceOwnerRef {
                id: Some(account.id),
                did: Some(account.did),
                name: Some(account.name),
            }),
            name: kamu_resources::ResourceName::new_unchecked(name),
            labels: Some(kamu_resources::ResourceLabels {
                entries: BTreeMap::new(),
            }),
            annotations: Some(kamu_resources::ResourceAnnotations {
                entries: BTreeMap::new(),
            }),
            owner_references: None,
        }
    }

    pub async fn get_snapshot_by_id(&self, id: &ResourceID) -> Option<ResourceSnapshot> {
        self.generic_query_svc.get_snapshot_by_id(id).await.unwrap()
    }

    pub async fn resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        name: &str,
    ) -> Option<ResourceID> {
        self.generic_query_svc
            .find_resource_id_by_name(
                account_id,
                schema,
                &kamu_resources::ResourceName::new_unchecked(name),
            )
            .await
            .unwrap()
    }

    pub fn expect_applied_messages(
        mock_outbox: &mut MockOutbox,
        n: usize,
        outcome: Option<ResourceLifecycleMessageOutcome>,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .times(n)
            .withf(move |producer, message, _version| {
                if producer != MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE {
                    return false;
                }
                let ResourceLifecycleMessage::Applied(m) =
                    serde_json::from_value(message.clone()).unwrap()
                else {
                    return false;
                };
                outcome.as_ref().is_none_or(|o| &m.outcome == o)
            })
            .returning(|_, _, _| Ok(()));
    }

    pub fn expect_deletion_requested_message(
        mock_outbox: &mut MockOutbox,
        expected_resource_count: usize,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .times(1)
            .withf(move |producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && matches!(
                        serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                            .unwrap(),
                        ResourceLifecycleMessage::DeletionRequested(ref m)
                            if m.resources.len() == expected_resource_count
                    )
            })
            .returning(|_, _, _| Ok(()));
    }

    pub fn expect_deleted_message(mock_outbox: &mut MockOutbox, expected_resource_count: usize) {
        Self::expect_deleted_messages(mock_outbox, 1, expected_resource_count);
    }

    /// Expects `n` `Deleted` messages, each of `expected_resource_count`
    /// resources
    pub fn expect_deleted_messages(
        mock_outbox: &mut MockOutbox,
        n: usize,
        expected_resource_count: usize,
    ) {
        mock_outbox
            .expect_post_message_as_json()
            .times(n)
            .withf(move |producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && matches!(
                        serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                            .unwrap(),
                        ResourceLifecycleMessage::Deleted(ref m)
                            if m.resources.len() == expected_resource_count
                    )
            })
            .returning(|_, _, _| Ok(()));
    }

    /// Expects one `StatusUpdated` message that leaves the resource in `phase`
    pub fn expect_status_updated_message(mock_outbox: &mut MockOutbox, phase: ResourcePhase) {
        mock_outbox
            .expect_post_message_as_json()
            .times(1)
            .withf(move |producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && matches!(
                        serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                            .unwrap(),
                        ResourceLifecycleMessage::StatusUpdated(ref m) if m.status.phase == phase
                    )
            })
            .returning(|_, _, _| Ok(()));
    }

    /// Asserts the code of the `ReconciliationError` condition in `status`,
    /// or that the condition is absent when `expected_code` is `None`
    #[track_caller]
    pub fn assert_reconciliation_error(
        resource_status: &ResourceStatus,
        expected_code: Option<&str>,
    ) {
        let error = resource_status
            .conditions
            .entries
            .get(&reconciliation_error_type_ref())
            .map(|value| {
                let proxy: odf::metadata::serde::yaml::resources::ReconciliationError =
                    serde_json::from_value(value.clone()).unwrap_or_else(|err| {
                        panic!("Malformed ReconciliationError {value}: {err}")
                    });
                ReconciliationError::try_from(proxy).unwrap()
            });

        assert_eq!(
            error.as_ref().map(|error| error.code.as_str()),
            expected_code,
            "ReconciliationError code mismatch: {error:?}"
        );

        if let Some(error) = error {
            assert!(
                !error.message.is_empty(),
                "ReconciliationError must carry a message"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
