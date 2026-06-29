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
use kamu_configuration::{DatasetSecretSetBindingRepository, DatasetVariableSetBindingRepository};
use kamu_configuration_inmem::{
    InMemoryDatasetSecretSetBindingRepository,
    InMemoryDatasetVariableSetBindingRepository,
};
use kamu_configuration_services::ConfigurationDatasetLifecycleMessageConsumer;
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_resources::ResourceID;
use messaging_outbox::{MessageConsumerT, OutboxProvider, register_message_dispatcher};
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted_clears_variable_and_secret_bindings() {
    let harness = ConfigurationDatasetLifecycleConsumerHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let uid_var = ResourceID::new(Uuid::new_v4());
    let uid_sec = ResourceID::new(Uuid::new_v4());

    // Seed a variable-set binding and a secret-set binding for the dataset
    harness
        .variable_binding_repo()
        .replace_bindings(&dataset_id, &[uid_var])
        .await
        .unwrap();
    harness
        .secret_binding_repo()
        .replace_bindings(&dataset_id, &[uid_sec])
        .await
        .unwrap();

    assert_eq!(
        harness
            .variable_binding_repo()
            .list_bindings(&dataset_id)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        harness
            .secret_binding_repo()
            .list_bindings(&dataset_id)
            .await
            .unwrap()
            .len(),
        1
    );

    // Consume a Deleted message for the dataset
    harness
        .consume_message(&DatasetLifecycleMessage::deleted(
            Utc::now(),
            dataset_id.clone(),
        ))
        .await
        .unwrap();

    assert_eq!(
        harness
            .variable_binding_repo()
            .list_bindings(&dataset_id)
            .await
            .unwrap()
            .len(),
        0,
        "variable-set bindings must be cleared on dataset deletion"
    );
    assert_eq!(
        harness
            .secret_binding_repo()
            .list_bindings(&dataset_id)
            .await
            .unwrap()
            .len(),
        0,
        "secret-set bindings must be cleared on dataset deletion"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_non_deleted_message_is_no_op() {
    let harness = ConfigurationDatasetLifecycleConsumerHarness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, account_id) = odf::AccountID::new_generated_ed25519();
    let uid_var = ResourceID::new(Uuid::new_v4());

    harness
        .variable_binding_repo()
        .replace_bindings(&dataset_id, &[uid_var])
        .await
        .unwrap();

    // Consume a Created message — must be a no-op
    harness
        .consume_message(&DatasetLifecycleMessage::created(
            Utc::now(),
            dataset_id.clone(),
            account_id,
            odf::DatasetVisibility::Public,
            odf::DatasetName::new_unchecked("test-dataset"),
        ))
        .await
        .unwrap();

    // Bindings must be unchanged
    assert_eq!(
        harness
            .variable_binding_repo()
            .list_bindings(&dataset_id)
            .await
            .unwrap()
            .len(),
        1,
        "bindings must not be touched by a non-Deleted message"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleting_one_dataset_does_not_affect_other_datasets_bindings() {
    let harness = ConfigurationDatasetLifecycleConsumerHarness::new();

    let (_, dataset_a) = odf::DatasetID::new_generated_ed25519();
    let (_, dataset_b) = odf::DatasetID::new_generated_ed25519();
    let id_a_var = ResourceID::new(Uuid::new_v4());
    let id_a_sec = ResourceID::new(Uuid::new_v4());
    let id_b_var = ResourceID::new(Uuid::new_v4());

    // Bind both datasets
    harness
        .variable_binding_repo()
        .replace_bindings(&dataset_a, &[id_a_var])
        .await
        .unwrap();
    harness
        .secret_binding_repo()
        .replace_bindings(&dataset_a, &[id_a_sec])
        .await
        .unwrap();
    harness
        .variable_binding_repo()
        .replace_bindings(&dataset_b, &[id_b_var])
        .await
        .unwrap();

    // Delete only dataset_a
    harness
        .consume_message(&DatasetLifecycleMessage::deleted(
            Utc::now(),
            dataset_a.clone(),
        ))
        .await
        .unwrap();

    // dataset_a bindings must be gone
    assert_eq!(
        harness
            .variable_binding_repo()
            .list_bindings(&dataset_a)
            .await
            .unwrap()
            .len(),
        0,
        "dataset_a variable binding must be cleared"
    );
    assert_eq!(
        harness
            .secret_binding_repo()
            .list_bindings(&dataset_a)
            .await
            .unwrap()
            .len(),
        0,
        "dataset_a secret binding must be cleared"
    );

    // dataset_b binding must be untouched
    assert_eq!(
        harness
            .variable_binding_repo()
            .list_bindings(&dataset_b)
            .await
            .unwrap()
            .len(),
        1,
        "dataset_b variable binding must be unaffected"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ConfigurationDatasetLifecycleConsumerHarness {
    catalog: dill::Catalog,
}

impl ConfigurationDatasetLifecycleConsumerHarness {
    fn new() -> Self {
        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add::<InMemoryDatasetVariableSetBindingRepository>();
        b.add::<InMemoryDatasetSecretSetBindingRepository>();
        b.add::<ConfigurationDatasetLifecycleMessageConsumer>();

        register_message_dispatcher::<DatasetLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        );

        Self { catalog: b.build() }
    }

    fn variable_binding_repo(&self) -> Arc<dyn DatasetVariableSetBindingRepository> {
        self.catalog.get_one().unwrap()
    }

    fn secret_binding_repo(&self) -> Arc<dyn DatasetSecretSetBindingRepository> {
        self.catalog.get_one().unwrap()
    }

    async fn consume_message(
        &self,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        self.catalog
            .get_one::<dyn MessageConsumerT<DatasetLifecycleMessage>>()
            .unwrap()
            .consume_message(&self.catalog, message)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
