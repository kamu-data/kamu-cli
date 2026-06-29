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
use database_common::NoOpDatabasePlugin;
use dill::CatalogBuilder;
use kamu_resources::{
    GenericResourceQueryService,
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceConditionStatus,
    ResourceConditionType,
    ResourceHeadersInput,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceRepository,
    ResourceSnapshot,
    ResourceStatus,
};
use kamu_resources_inmem::{InMemoryRawResourceEventStore, InMemoryResourceRepository};
use messaging_outbox::{MockOutbox, Outbox, OutboxProvider, register_message_dispatcher};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

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
}

pub struct BaseResourceServiceHarnessOpts {
    pub outbox_provider: OutboxProvider,
}

impl Default for BaseResourceServiceHarnessOpts {
    fn default() -> Self {
        Self {
            outbox_provider: OutboxProvider::Immediate {
                force_immediate: true,
            },
        }
    }
}

impl BaseResourceServiceHarness {
    pub fn new() -> Self {
        Self::new_with_opts(BaseResourceServiceHarnessOpts::default())
    }

    pub fn new_with_opts(opts: BaseResourceServiceHarnessOpts) -> Self {
        let mut b = CatalogBuilder::new();

        b.add_value(SystemTimeSourceStub::new_set(Utc::now()))
            .bind::<dyn SystemTimeSource, SystemTimeSourceStub>();

        let needs_bridge = matches!(opts.outbox_provider, OutboxProvider::Dispatching);
        opts.outbox_provider.embed_into_catalog(&mut b);

        if needs_bridge {
            b.add::<kamu_messaging_outbox_inmem::InMemoryOutboxMessageBridge>();
        }

        b.add::<InMemoryResourceRepository>()
            .add::<InMemoryRawResourceEventStore>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        crate::register_dependencies(&mut b);

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

    pub fn make_headers_input(account_id: odf::AccountID, name: &str) -> ResourceHeadersInput {
        ResourceHeadersInput {
            account: account_id,
            name: name.to_string(),
            description: None,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
        }
    }

    pub async fn get_snapshot_by_id(&self, id: &ResourceID) -> Option<ResourceSnapshot> {
        self.generic_query_svc.get_snapshot_by_id(id).await.unwrap()
    }

    pub async fn resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &str,
    ) -> Option<ResourceID> {
        self.generic_query_svc
            .find_resource_id_by_name(account_id, kind, &name.to_string())
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
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && match serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                        .unwrap()
                    {
                        ResourceLifecycleMessage::Applied(ref m) => {
                            outcome.as_ref().is_none_or(|o| &m.outcome == o)
                        }
                        _ => false,
                    }
            })
            .returning(|_, _, _| Ok(()));
    }

    pub fn expect_deleted_message(mock_outbox: &mut MockOutbox, expected_resource_count: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .times(1)
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

    pub fn expect_reconciliation_succeeded_message(mock_outbox: &mut MockOutbox, n: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .times(n)
            .withf(move |producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && matches!(
                        serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                            .unwrap(),
                        ResourceLifecycleMessage::ReconciliationSucceeded(_)
                    )
            })
            .returning(|_, _, _| Ok(()));
    }

    pub fn expect_reconciliation_failed_message(mock_outbox: &mut MockOutbox, n: usize) {
        mock_outbox
            .expect_post_message_as_json()
            .times(n)
            .withf(move |producer, message, _version| {
                producer == MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE
                    && matches!(
                        serde_json::from_value::<ResourceLifecycleMessage>(message.clone())
                            .unwrap(),
                        ResourceLifecycleMessage::ReconciliationFailed(_)
                    )
            })
            .returning(|_, _, _| Ok(()));
    }

    /// Assert that a condition of `type_` is present in `status` and has the
    /// expected `status` value.  Optionally assert `reason` and whether a
    /// non-None `message` is present.
    #[track_caller]
    pub fn assert_condition(
        resource_status: &ResourceStatus,
        type_: ResourceConditionType,
        expected_status: ResourceConditionStatus,
        expected_reason: Option<&str>,
        expect_message: Option<bool>,
    ) {
        let cond = resource_status
            .conditions
            .iter()
            .find(|c| c.type_ == type_)
            .unwrap_or_else(|| panic!("{type_:?} condition must be present in status"));

        assert_eq!(
            cond.status, expected_status,
            "{type_:?} condition status mismatch"
        );

        if let Some(reason) = expected_reason {
            assert_eq!(cond.reason, reason, "{type_:?} condition reason mismatch");
        }

        if let Some(should_have_message) = expect_message {
            if should_have_message {
                assert!(
                    cond.message.is_some(),
                    "{type_:?} condition must carry a message"
                );
            } else {
                assert!(
                    cond.message.is_none(),
                    "{type_:?} condition must not carry a message"
                );
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
