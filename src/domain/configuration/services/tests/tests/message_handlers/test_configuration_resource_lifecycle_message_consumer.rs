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
    ResourceID,
    ResourceLifecycleMessage,
    ResourceSnapshot,
};
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reconciliation_succeeded_for_variable_set_cleans_up_old_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    // Seed entries for generation 1
    harness
        .variable_repo()
        .replace_entries(
            &id,
            1,
            &[kamu_configuration::VariableSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: "K".to_string(),
                value: "v1".to_string(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    // Seed entries for generation 2
    harness
        .variable_repo()
        .replace_entries(
            &id,
            2,
            &[kamu_configuration::VariableSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: "K".to_string(),
                value: "v2".to_string(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    // Consume ReconciliationSucceeded for generation 2 → should clean up gen 1
    harness
        .consume_message(&ResourceLifecycleMessage::reconciliation_succeeded(
            Utc::now(),
            make_snapshot(
                id,
                VariableSetResource::RESOURCE_TYPE,
                VariableSetResource::API_VERSION,
                2,
            ),
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
async fn test_reconciliation_succeeded_for_secret_set_cleans_up_old_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    harness
        .secret_repo()
        .replace_entries(
            &id,
            1,
            &[kamu_configuration::SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
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
                account_id: account_id.clone(),
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
        .consume_message(&ResourceLifecycleMessage::reconciliation_succeeded(
            Utc::now(),
            make_snapshot(
                id,
                SecretSetResource::RESOURCE_TYPE,
                SecretSetResource::API_VERSION,
                2,
            ),
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
async fn test_reconciliation_succeeded_for_unknown_kind_is_no_op() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();

    // Must succeed without error
    harness
        .consume_message(&ResourceLifecycleMessage::reconciliation_succeeded(
            Utc::now(),
            make_snapshot(id, "UnknownKind", "unknown.dev/v1", 1),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_for_variable_set_removes_all_projection_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    harness
        .variable_repo()
        .replace_entries(
            &id,
            1,
            &[kamu_configuration::VariableSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: "K".to_string(),
                value: "v".to_string(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![make_snapshot(
                id,
                VariableSetResource::RESOURCE_TYPE,
                VariableSetResource::API_VERSION,
                1,
            )],
        ))
        .await
        .unwrap();

    let remaining = harness
        .variable_repo()
        .get_latest_entries(&id)
        .await
        .unwrap();
    assert!(remaining.is_empty(), "all variable entries must be deleted");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_for_secret_set_removes_all_projection_entries() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    harness
        .secret_repo()
        .replace_entries(
            &id,
            1,
            &[kamu_configuration::SecretSetEntry {
                entry_id: Uuid::new_v4(),
                account_id: account_id.clone(),
                key: "S".to_string(),
                value: b"enc".to_vec(),
                secret_nonce: b"n".to_vec(),
                created_at: now,
                updated_at: now,
            }],
        )
        .await
        .unwrap();

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![make_snapshot(
                id,
                SecretSetResource::RESOURCE_TYPE,
                SecretSetResource::API_VERSION,
                1,
            )],
        ))
        .await
        .unwrap();

    let remaining = harness.secret_repo().get_latest_entries(&id).await.unwrap();
    assert!(remaining.is_empty(), "all secret entries must be deleted");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_for_variable_set_with_multiple_ids_removes_all() {
    // All UIDs passed in the same Deleted message are deleted together
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id_a = harness.alloc_id();
    let id_b = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    for id in [id_a, id_b] {
        harness
            .variable_repo()
            .replace_entries(
                &id,
                1,
                &[kamu_configuration::VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "K".to_string(),
                    value: "v".to_string(),
                    created_at: now,
                    updated_at: now,
                }],
            )
            .await
            .unwrap();
    }

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![
                make_snapshot(
                    id_a,
                    VariableSetResource::RESOURCE_TYPE,
                    VariableSetResource::API_VERSION,
                    1,
                ),
                make_snapshot(
                    id_b,
                    VariableSetResource::RESOURCE_TYPE,
                    VariableSetResource::API_VERSION,
                    1,
                ),
            ],
        ))
        .await
        .unwrap();

    for id in [id_a, id_b] {
        assert!(
            harness
                .variable_repo()
                .get_latest_entries(&id)
                .await
                .unwrap()
                .is_empty(),
            "entries for id {id} must be deleted"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_for_variable_set_removes_only_targeted_id() {
    // Only the UID(s) named in the Deleted message lose their entries;
    // unrelated UIDs (e.g. from a different dataset's binding) are untouched.
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id_deleted = harness.alloc_id();
    let id_other = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    for id in [id_deleted, id_other] {
        harness
            .variable_repo()
            .replace_entries(
                &id,
                1,
                &[kamu_configuration::VariableSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "K".to_string(),
                    value: "v".to_string(),
                    created_at: now,
                    updated_at: now,
                }],
            )
            .await
            .unwrap();
    }

    // Delete only uid_deleted
    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![make_snapshot(
                id_deleted,
                VariableSetResource::RESOURCE_TYPE,
                VariableSetResource::API_VERSION,
                1,
            )],
        ))
        .await
        .unwrap();

    assert!(
        harness
            .variable_repo()
            .get_latest_entries(&id_deleted)
            .await
            .unwrap()
            .is_empty(),
        "targeted id entries must be deleted"
    );
    assert_eq!(
        harness
            .variable_repo()
            .get_latest_entries(&id_other)
            .await
            .unwrap()
            .len(),
        1,
        "unrelated id entries must be untouched"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_for_secret_set_with_multiple_ids_removes_all() {
    let harness = ConfigurationResourceLifecycleConsumerHarness::new();
    let id_a = harness.alloc_id();
    let id_b = harness.alloc_id();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();

    let now = Utc::now();

    for id in [id_a, id_b] {
        harness
            .secret_repo()
            .replace_entries(
                &id,
                1,
                &[kamu_configuration::SecretSetEntry {
                    entry_id: Uuid::new_v4(),
                    account_id: account_id.clone(),
                    key: "S".to_string(),
                    value: b"enc".to_vec(),
                    secret_nonce: b"n".to_vec(),
                    created_at: now,
                    updated_at: now,
                }],
            )
            .await
            .unwrap();
    }

    harness
        .consume_message(&ResourceLifecycleMessage::deleted(
            Utc::now(),
            vec![
                make_snapshot(
                    id_a,
                    SecretSetResource::RESOURCE_TYPE,
                    SecretSetResource::API_VERSION,
                    1,
                ),
                make_snapshot(
                    id_b,
                    SecretSetResource::RESOURCE_TYPE,
                    SecretSetResource::API_VERSION,
                    1,
                ),
            ],
        ))
        .await
        .unwrap();

    for id in [id_a, id_b] {
        assert!(
            harness
                .secret_repo()
                .get_latest_entries(&id)
                .await
                .unwrap()
                .is_empty(),
            "entries for id {id} must be deleted"
        );
    }
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
                VariableSetResource::RESOURCE_TYPE,
                VariableSetResource::API_VERSION,
                1,
            ),
        ))
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn make_snapshot(
    id: ResourceID,
    kind: &str,
    api_version: &str,
    generation: u64,
) -> ResourceSnapshot {
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    let now = Utc::now();
    let mut headers = ResourceHeaders::simple(now, account_id, "test-res");
    headers.generation = generation;
    ResourceSnapshot {
        id,
        kind: kind.to_string(),
        api_version: api_version.to_string(),
        headers,
        spec: serde_json::json!({}),
        status: None,
        last_reconciled_at: None,
        last_event_id: None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConfigurationResourceLifecycleConsumerHarness {
    catalog: dill::Catalog,
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

        Self { catalog: b.build() }
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
