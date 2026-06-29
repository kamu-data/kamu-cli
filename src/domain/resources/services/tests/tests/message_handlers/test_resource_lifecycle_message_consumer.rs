// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use dill::CatalogBuilder;
use internal_error::InternalError;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ReconcileResourceUseCase,
    ReconcileResourceUseCaseError,
    ResourceApiVersion,
    ResourceHeaders,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageOutcome,
    ResourceSnapshot,
    ResourceType,
    ResourceUID,
};
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use mockall::mock;

use crate::tests::utils::{
    TestResource,
    TestResourceResourceLifecycleDispatcher,
    make_account_id,
    make_uid,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Mock
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Not using automock, because it's a generic trait
mock! {
    pub TestResourceReconcileUseCase {}

    #[async_trait::async_trait]
    impl ReconcileResourceUseCase<TestResource> for TestResourceReconcileUseCase {
        async fn execute(
            &self,
            id: &ResourceUID,
        ) -> Result<(), ReconcileResourceUseCaseError<TestResource>>;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_triggers_reconciliation() {
    // Applied message → ResourceLifecycleMessageConsumer → handle_applied →
    // ReconcileResourceUseCase::execute.  Verified via mock expectations.
    let uid = make_uid();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_execute_once(uid),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot(uid),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_message_is_no_op_for_reconciler() {
    // Deleted message → handle_deleted, which is a no-op in the generated
    // reconcile dispatcher.  Verified: reconcile use case is never called.
    let uid = make_uid();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_no_execute(),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![ResourceLifecycleConsumerHarness::make_snapshot(uid)],
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_with_unregistered_kind_returns_error() {
    // Applied message for an unknown kind: dispatcher lookup fails because no
    // lifecycle dispatcher is registered for "UnknownKind".
    let uid = make_uid();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_no_execute(),
    );

    let result = harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot_with_kind(
                uid,
                "UnknownKind",
                "unknown.dev/v1",
            ),
        ))
        .await;

    assert!(
        result.is_err(),
        "consumer must return an error for an unregistered resource kind"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceLifecycleConsumerHarness {
    catalog: dill::Catalog,
}

impl ResourceLifecycleConsumerHarness {
    fn expect_execute_once(uid: ResourceUID) -> MockTestResourceReconcileUseCase {
        let mut mock = MockTestResourceReconcileUseCase::new();
        mock.expect_execute()
            .once()
            .withf(move |id| *id == uid)
            .returning(|_| Ok(()));
        mock
    }

    fn expect_no_execute() -> MockTestResourceReconcileUseCase {
        let mut mock = MockTestResourceReconcileUseCase::new();
        mock.expect_execute().never();
        mock
    }

    fn new(mock_reconcile_uc: MockTestResourceReconcileUseCase) -> Self {
        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add_value(mock_reconcile_uc)
            .bind::<dyn ReconcileResourceUseCase<TestResource>, MockTestResourceReconcileUseCase>();

        b.add::<TestResourceResourceLifecycleDispatcher>();
        b.add::<kamu_resources_services::ResourceLifecycleMessageConsumer>();

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();
        Self { catalog }
    }

    async fn consume_message(
        &self,
        message: &ResourceLifecycleMessage,
    ) -> Result<(), InternalError> {
        self.catalog
            .get_one::<dyn MessageConsumerT<ResourceLifecycleMessage>>()
            .unwrap()
            .consume_message(&self.catalog, message)
            .await
    }

    fn make_snapshot(uid: ResourceUID) -> ResourceSnapshot {
        Self::make_snapshot_with_kind(uid, TestResource::RESOURCE_TYPE, TestResource::API_VERSION)
    }

    fn make_snapshot_with_kind(
        uid: ResourceUID,
        kind: &str,
        api_version: &str,
    ) -> ResourceSnapshot {
        ResourceSnapshot {
            uid,
            kind: kind.to_string(),
            api_version: api_version.to_string(),
            headers: ResourceHeaders::simple(Utc::now(), make_account_id(), "res"),
            spec: serde_json::json!({ "value": "x" }),
            status: None,
            last_reconciled_at: None,
            last_event_id: None,
        }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
