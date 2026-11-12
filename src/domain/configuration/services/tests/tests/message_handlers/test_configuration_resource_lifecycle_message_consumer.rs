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

use chrono::{DateTime, TimeDelta, Utc};
use dill::CatalogBuilder;
use internal_error::InternalError;
use kamu_configuration::{
    SecretSetProjectionRepository,
    SecretSetResource,
    VariableSetProjectionRepository,
    VariableSetResource,
};
use kamu_configuration_inmem::{
    InMemorySecretSetProjectionRepository,
    InMemoryVariableSetProjectionRepository,
};
use kamu_configuration_services::ConfigurationResourceLifecycleMessageConsumer;
use kamu_resources::{
    MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceLifecycleMessage,
    ResourceLifecycleMessageResourceRef,
    ResourcePhase,
    ResourceSchemaProvider,
    ResourceSnapshot,
    ResourceStatus,
    TypeUri,
    new_pending_resource_status,
};
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciled_variable_set_cleans_up_old_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    harness.seed_variable_entries(&id, 1).await;
    harness.seed_variable_entries(&id, 2).await;
    let now = Utc::now();

    // Reconciling generation 2 supersedes the entries of generation 1
    harness
        .consume_message(&make_status_updated(
            id,
            VariableSetResource::schema(),
            &harness.account_handle,
            2,
            now,
            &reconciled_status(2, now),
        ))
        .await
        .unwrap();

    // Generation 1 entries must be gone
    let gen1 = harness.variable_repo().get_entries(&id, 1).await.unwrap();
    assert!(gen1.is_empty(), "gen-1 entries must be cleaned up");

    // Generation 2 entries must still be present
    let gen2 = harness.variable_repo().get_entries(&id, 2).await.unwrap();
    assert_eq!(gen2.len(), 1, "gen-2 entries must survive cleanup");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciled_secret_set_cleans_up_old_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();

    let now = Utc::now();

    harness
        .secret_repo()
        .replace_entries(
            &id,
            1,
            &[kamu_configuration::SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: harness.account_handle.did.clone(),
                key: "S".to_string(),
                value: b"enc1".to_vec(),
                secret_nonce: b"n1".to_vec(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    harness
        .secret_repo()
        .replace_entries(
            &id,
            2,
            &[kamu_configuration::SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: harness.account_handle.did.clone(),
                key: "S".to_string(),
                value: b"enc2".to_vec(),
                secret_nonce: b"n2".to_vec(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    harness
        .consume_message(&make_status_updated(
            id,
            SecretSetResource::schema(),
            &harness.account_handle,
            2,
            now,
            &reconciled_status(2, now),
        ))
        .await
        .unwrap();

    let gen1 = harness.secret_repo().get_entries(&id, 1).await.unwrap();
    assert!(gen1.is_empty(), "gen-1 entries must be cleaned up");

    let gen2 = harness.secret_repo().get_entries(&id, 2).await.unwrap();
    assert_eq!(gen2.len(), 1, "gen-2 entries must survive cleanup");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciled_unknown_kind_is_no_op() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();

    static UNKNOWN_SCHEMA: std::sync::LazyLock<TypeUri> =
        std::sync::LazyLock::new(|| TypeUri::new_unchecked("unknown"));
    let now = Utc::now();

    // Must succeed without error
    harness
        .consume_message(&make_status_updated(
            id,
            &UNKNOWN_SCHEMA,
            &harness.account_handle,
            1,
            now,
            &reconciled_status(1, now),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_applied_message_is_no_op() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();

    harness
        .consume_message(&ResourceLifecycleMessage::applied(
            Utc::now(),
            kamu_resources::ResourceLifecycleMessageOutcome::Created,
            make_snapshot(
                id,
                VariableSetResource::schema(),
                &harness.account_handle,
                1,
            ),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_status_update_not_recording_reconciliation_of_current_generation_keeps_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    harness.seed_variable_entries(&id, 1).await;
    harness.seed_variable_entries(&id, 2).await;

    // None records the reconciliation of generation 2 at the message's time;
    // the last is a later update that keeps an earlier `reconciledAt`
    let now = Utc::now();
    for status in [
        reconciling_status(2),
        reconciled_status(1, now),
        reconciled_status(2, now - TimeDelta::minutes(1)),
    ] {
        harness
            .consume_message(&make_status_updated(
                id,
                VariableSetResource::schema(),
                &harness.account_handle,
                2,
                now,
                &status,
            ))
            .await
            .unwrap();
    }

    let gen1 = harness.variable_repo().get_entries(&id, 1).await.unwrap();
    assert_eq!(
        gen1.len(),
        1,
        "gen-1 entries must stay until gen 2 is reconciled"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_status_updated(
    id: ResourceID,
    schema: &'static TypeUri,
    account: &odf::ResourceOwnerHandle,
    generation: u64,
    event_time: DateTime<Utc>,
    status: &ResourceStatus,
) -> ResourceLifecycleMessage {
    let snapshot = make_snapshot(id, schema, account, generation);
    ResourceLifecycleMessage::status_updated(
        event_time,
        ResourceLifecycleMessageResourceRef {
            id,
            schema: snapshot.schema,
            headers: snapshot.headers,
        },
        status,
        BTreeMap::new(),
    )
}

/// The status a reconciliation of `reconciled_generation` records at
/// `reconciled_at`, the time of its message
fn reconciled_status(reconciled_generation: u64, reconciled_at: DateTime<Utc>) -> ResourceStatus {
    ResourceStatus {
        phase: ResourcePhase::Ready,
        observed_generation: Some(reconciled_generation),
        reconciled_generation: Some(reconciled_generation),
        reconciled_at: Some(reconciled_at),
        ..new_pending_resource_status()
    }
}

fn reconciling_status(observed_generation: u64) -> ResourceStatus {
    ResourceStatus {
        phase: ResourcePhase::Reconciling,
        observed_generation: Some(observed_generation),
        ..new_pending_resource_status()
    }
}

fn make_snapshot(
    id: ResourceID,
    schema: &'static TypeUri,
    account: &odf::ResourceOwnerHandle,
    generation: u64,
) -> ResourceSnapshot {
    let now = Utc::now();
    let mut headers = ResourceHeaders::simple(now, id, account.clone(), "test-res");
    headers.generation = generation;
    ResourceSnapshot {
        id,
        schema: schema.clone(),
        headers,
        spec: serde_json::json!({}),
        status: None,
        last_event_id: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConfigurationResourceLifecycleConsumerHarness {
    catalog: dill::Catalog,
    account_handle: odf::ResourceOwnerHandle,
}

impl ConfigurationResourceLifecycleConsumerHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add::<InMemoryVariableSetProjectionRepository>();
        b.add::<InMemorySecretSetProjectionRepository>();
        b.add::<ConfigurationResourceLifecycleMessageConsumer>();

        register_message_dispatcher::<ResourceLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_RESOURCE_SERVICE,
        );

        Self {
            catalog: b.build(),
            account_handle: odf::ResourceOwnerHandle::new_test("test-owner"),
        }
    }

    fn alloc_id(&self) -> ResourceID {
        ResourceID::new(Uuid::new_v4())
    }

    fn variable_repo(&self) -> Arc<dyn VariableSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    fn secret_repo(&self) -> Arc<dyn SecretSetProjectionRepository> {
        self.catalog.get_one().unwrap()
    }

    /// Stores one variable entry for `generation`
    async fn seed_variable_entries(&self, id: &ResourceID, generation: u64) {
        let now = Utc::now();
        self.variable_repo()
            .replace_entries(
                id,
                generation,
                &[kamu_configuration::VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: self.account_handle.did.clone(),
                    key: "K".to_string(),
                    value: format!("v{generation}"),
                    created_at: now,
                    updated_at: now,
                }],
            )
            .await
            .unwrap();
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
