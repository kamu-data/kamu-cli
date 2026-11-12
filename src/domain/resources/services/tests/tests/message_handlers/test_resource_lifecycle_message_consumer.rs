// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use chrono::Utc;
use dill::CatalogBuilder;
use internal_error::InternalError;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceControllerHints,
    ResourceControllerName,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageConditionChange,
    ResourceLifecycleMessageOutcome,
    ResourceLifecycleMessageResourceRef,
    ResourceSchemaProvider,
    ResourceSnapshot,
    new_pending_resource_status,
};
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use odf::metadata::resources::TypeUri;
use pretty_assertions::assert_eq;
use wakeup_listener::WakeupListener;

use crate::tests::utils::{TestResource, make_id};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_hints_its_schema() {
    let harness = ResourceLifecycleConsumerHarness::new();
    let schema = TypeUri::new_unchecked("ControllerDrivenSchema");

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            ResourceLifecycleMessageOutcome::Created,
            ResourceLifecycleConsumerHarness::make_snapshot_with_schema(make_id(), &schema),
        ))
        .await
        .unwrap();

    assert_eq!(harness.hinted_schemas(), vec![schema]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deletion_requested_message_hints_once_per_batch() {
    let harness = ResourceLifecycleConsumerHarness::new();

    harness
        .consume_message(&ResourceLifecycleMessage::deletion_requested(
            Utc::now(),
            vec![
                ResourceLifecycleConsumerHarness::make_snapshot(make_id()),
                ResourceLifecycleConsumerHarness::make_snapshot(make_id()),
            ],
        ))
        .await
        .unwrap();

    assert_eq!(
        harness.hinted_schemas(),
        vec![TestResource::schema().clone()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_updated_message_hints_its_schema() {
    let harness = ResourceLifecycleConsumerHarness::new();

    harness
        .consume_message(&ResourceLifecycleConsumerHarness::make_status_updated(
            make_id(),
        ))
        .await
        .unwrap();

    assert_eq!(
        harness.hinted_schemas(),
        vec![TestResource::schema().clone()]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_message_hints_nothing() {
    let harness = ResourceLifecycleConsumerHarness::new();

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![ResourceLifecycleConsumerHarness::make_snapshot(make_id())],
        ))
        .await
        .unwrap();

    assert_eq!(harness.hinted_schemas(), vec![]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ResourceLifecycleConsumerHarness {
    catalog: dill::Catalog,
}

impl ResourceLifecycleConsumerHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add_value(RecordingHints::default())
            .bind::<dyn ResourceControllerHints, RecordingHints>();
        b.add::<kamu_resources_services::ResourceLifecycleMessageConsumer>();

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        let catalog = b.build();
        Self { catalog }
    }

    /// The schemas the consumer hinted, in order
    fn hinted_schemas(&self) -> Vec<TypeUri> {
        self.catalog
            .get_one::<RecordingHints>()
            .unwrap()
            .schemas
            .lock()
            .unwrap()
            .clone()
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

/// Records the schemas hinted to the controller runtime
#[derive(Default)]
struct RecordingHints {
    schemas: Mutex<Vec<TypeUri>>,
}

impl ResourceControllerHints for RecordingHints {
    fn hint(&self, schema: &TypeUri) {
        self.schemas.lock().unwrap().push(schema.clone());
    }

    fn new_wakeup_listener(
        &self,
        _schema: &'static str,
        _controller_name: &ResourceControllerName,
    ) -> Box<dyn WakeupListener> {
        unreachable!("the consumer never listens")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
