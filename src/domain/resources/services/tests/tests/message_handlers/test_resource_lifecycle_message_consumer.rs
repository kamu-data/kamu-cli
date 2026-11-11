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
use event_sourcing::ConcurrentModificationError;
use internal_error::InternalError;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    MockSimpleResourceController,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageConditionChange,
    ResourceLifecycleMessageOutcome,
    ResourceLifecycleMessageResourceRef,
    ResourceSchemaProvider,
    ResourceSnapshot,
    ResourceStatusConflictError,
    SimpleResourceController,
    SimpleResourceDeletionError,
    SimpleResourceReconciliationError,
    new_pending_resource_status,
};
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use odf::metadata::resources::TypeUri;

use crate::tests::utils::{TestResource, TestResourceResourceLifecycleDispatcher, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_triggers_reconciliation() {
    // Applied message → ResourceLifecycleMessageConsumer → handle_applied →
    // SimpleResourceController::reconcile.  Verified via mock expectations.
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_reconcile_once(id),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot(id),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_skips_reconciliation_status_conflict() {
    // A concurrent reconciliation of the same resource already recorded its
    // outcome, so the conflict is not an error of the message.
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_reconcile_once_with_status_conflict(id),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot(id),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_skips_reconciliation_concurrent_modification() {
    // The writer the repository detected reconciles the resource itself or
    // makes the reconciliation unnecessary
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_reconcile_once_with_concurrent_modification(id),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot(id),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deletion_requested_message_completes_deletions_in_one_batch() {
    let id_a = make_id();
    let id_b = make_id();

    let harness = ResourceLifecycleConsumerHarness::new_with_deletion(
        ResourceLifecycleConsumerHarness::expect_no_reconcile(),
        ResourceLifecycleConsumerHarness::expect_complete_deletion(&[id_a, id_b], || Ok(())),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::deletion_requested(
            Utc::now(),
            vec![
                ResourceLifecycleConsumerHarness::make_snapshot(id_a),
                ResourceLifecycleConsumerHarness::make_snapshot(id_b),
            ],
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deletion_requested_message_skips_concurrent_modification() {
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new_with_deletion(
        ResourceLifecycleConsumerHarness::expect_no_reconcile(),
        ResourceLifecycleConsumerHarness::expect_complete_deletion(&[id], || {
            Err(SimpleResourceDeletionError::ConcurrentModification(
                ConcurrentModificationError {},
            ))
        }),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::deletion_requested(
            Utc::now(),
            vec![ResourceLifecycleConsumerHarness::make_snapshot(id)],
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_message_is_no_op_for_controller() {
    // Deleted message → handle_deleted, which is a no-op in the generated
    // controller dispatcher: the controller already cleaned up.
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_no_reconcile(),
    );

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![ResourceLifecycleConsumerHarness::make_snapshot(id)],
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_updated_message_is_no_op_for_controller() {
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_no_reconcile(),
    );

    harness
        .consume_message(&ResourceLifecycleConsumerHarness::make_status_updated(id))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_with_unregistered_schema_returns_error() {
    // Applied message for an unknown schema: dispatcher lookup fails because no
    // lifecycle dispatcher is registered for "UnknownSchema".
    let id = make_id();

    let harness = ResourceLifecycleConsumerHarness::new(
        ResourceLifecycleConsumerHarness::expect_no_reconcile(),
    );

    let result = harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot_with_schema(
                id,
                &TypeUri::new_unchecked("UnknownSchema"),
            ),
        ))
        .await;

    assert!(
        result.is_err(),
        "consumer must return an error for an unregistered resource schema"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceLifecycleConsumerHarness {
    catalog: dill::Catalog,
}

/// An expectation set on the mocked controller
type ControllerExpectation = Box<dyn FnOnce(&mut MockSimpleResourceController<TestResource>)>;

impl ResourceLifecycleConsumerHarness {
    fn expect_reconcile_once(id: ResourceID) -> ControllerExpectation {
        Box::new(move |mock| {
            mock.expect_reconcile()
                .once()
                .withf(move |id_| *id_ == id)
                .returning(|_| Ok(()));
        })
    }

    fn expect_reconcile_once_with_status_conflict(id: ResourceID) -> ControllerExpectation {
        Box::new(move |mock| {
            mock.expect_reconcile()
                .once()
                .withf(move |id_| *id_ == id)
                .returning(|_| {
                    Err(SimpleResourceReconciliationError::StatusConflict(
                        ResourceStatusConflictError {
                            expected: 2,
                            actual: 3,
                        },
                    ))
                });
        })
    }

    fn expect_reconcile_once_with_concurrent_modification(id: ResourceID) -> ControllerExpectation {
        Box::new(move |mock| {
            mock.expect_reconcile()
                .once()
                .withf(move |id_| *id_ == id)
                .returning(|_| {
                    Err(SimpleResourceReconciliationError::ConcurrentModification(
                        ConcurrentModificationError {},
                    ))
                });
        })
    }

    fn expect_no_reconcile() -> ControllerExpectation {
        Box::new(|mock| {
            mock.expect_reconcile().never();
        })
    }

    fn expect_complete_deletion(
        ids: &[ResourceID],
        outcome: fn() -> Result<(), SimpleResourceDeletionError<TestResource>>,
    ) -> ControllerExpectation {
        let ids = ids.to_vec();
        Box::new(move |mock| {
            mock.expect_complete_deletions()
                .once()
                .withf(move |ids_| *ids_ == ids[..])
                .returning(move |_| outcome());
        })
    }

    fn expect_no_complete_deletion() -> ControllerExpectation {
        Box::new(|mock| {
            mock.expect_complete_deletions().never();
        })
    }

    fn new(reconcile: ControllerExpectation) -> Self {
        Self::new_with_deletion(reconcile, Self::expect_no_complete_deletion())
    }

    fn new_with_deletion(
        reconcile: ControllerExpectation,
        complete_deletion: ControllerExpectation,
    ) -> Self {
        let mut controller = MockSimpleResourceController::<TestResource>::new();
        reconcile(&mut controller);
        complete_deletion(&mut controller);

        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add_value(controller)
            .bind::<dyn SimpleResourceController<TestResource>, MockSimpleResourceController<TestResource>>();

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

    fn make_snapshot(id: ResourceID) -> ResourceSnapshot {
        Self::make_snapshot_with_schema(id, TestResource::schema())
    }

    fn make_status_updated(id: ResourceID) -> ResourceLifecycleMessage {
        let snapshot = Self::make_snapshot(id);
        ResourceLifecycleMessage::status_updated(
            Utc::now(),
            ResourceLifecycleMessageResourceRef {
                id,
                schema: snapshot.schema,
                headers: snapshot.headers,
            },
            &new_pending_resource_status(),
            [(
                TypeUri::new_unchecked(
                    "https://test.kamu.dev/schemas/test/v1/conditions/TestStage",
                ),
                ResourceLifecycleMessageConditionChange::Removed,
            )]
            .into(),
        )
    }

    fn make_snapshot_with_schema(id: ResourceID, schema: &TypeUri) -> ResourceSnapshot {
        let account_handle = odf::ResourceOwnerHandle::new_test("test-oowner");

        ResourceSnapshot {
            id,
            schema: schema.clone(),
            headers: ResourceHeaders::simple(Utc::now(), id, account_handle, "res"),
            spec: serde_json::json!({ "value": "x" }),
            status: None,
            last_event_id: None,
        }
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
