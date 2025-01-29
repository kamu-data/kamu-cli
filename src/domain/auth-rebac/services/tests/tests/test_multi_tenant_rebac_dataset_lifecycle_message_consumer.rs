// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use kamu_auth_rebac::{DatasetProperties, Entity, RebacRepository, RebacService};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{MultiTenantRebacDatasetLifecycleMessageConsumer, RebacServiceImpl};
use kamu_datasets::DatasetLifecycleMessage;
use messaging_outbox::{consume_deserialized_message, ConsumerFilter, Message};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = MultiTenantRebacDatasetLifecycleMessageConsumerHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rebac_properties_added() {
    let harness = Harness::new();

    let (_, public_dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, private_dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, owner_id) = odf::AccountID::new_generated_ed25519();

    // Pre-checks
    {
        let public_dataset_entity = Entity::new_dataset(public_dataset_id.to_string());
        let private_dataset_entity = Entity::new_dataset(private_dataset_id.to_string());

        assert_matches!(
            harness
                .rebac_repo
                .get_entity_properties(&public_dataset_entity)
                .await
                .as_deref(),
            Ok([])
        );
        assert_matches!(
            harness
                .rebac_repo
                .get_entity_properties(&private_dataset_entity)
                .await
                .as_deref(),
            Ok([])
        );
    }

    // Simulate creations
    {
        harness
            .mimic(DatasetLifecycleMessage::created(
                public_dataset_id.clone(),
                owner_id.clone(),
                odf::DatasetVisibility::Public,
                odf::DatasetName::new_unchecked("public-dataset"),
            ))
            .await;
        harness
            .mimic(DatasetLifecycleMessage::created(
                private_dataset_id.clone(),
                owner_id,
                odf::DatasetVisibility::Private,
                odf::DatasetName::new_unchecked("private-dataset"),
            ))
            .await;
    }

    // Validate properties
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&public_dataset_id)
                .await,
            Ok(DatasetProperties {
                allows_anonymous_read: true,
                allows_public_read: true
            })
        );
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&private_dataset_id)
                .await,
            Ok(DatasetProperties {
                allows_anonymous_read: false,
                allows_public_read: false
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rebac_properties_deleted() {
    let harness = Harness::new();

    let (_, dataset_id) = odf::DatasetID::new_generated_ed25519();
    let (_, owner_id) = odf::AccountID::new_generated_ed25519();

    // Simulate creation
    {
        harness
            .mimic(DatasetLifecycleMessage::created(
                dataset_id.clone(),
                owner_id.clone(),
                odf::DatasetVisibility::Public,
                odf::DatasetName::new_unchecked("public-dataset"),
            ))
            .await;
    }

    // Validate properties
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id)
                .await,
            Ok(DatasetProperties {
                allows_anonymous_read: true,
                allows_public_read: true
            })
        );
    }

    // Simulate deletion
    {
        harness
            .mimic(DatasetLifecycleMessage::deleted(dataset_id.clone()))
            .await;
    }

    let dataset_entity = Entity::new_dataset(dataset_id.to_string());

    // Validate
    {
        assert_matches!(
            harness
                .rebac_repo
                .get_entity_properties(&dataset_entity)
                .await
                .as_deref(),
            Ok([])
        );
    }

    // Simulate deletion again to check idempotency
    {
        harness
            .mimic(DatasetLifecycleMessage::deleted(dataset_id.clone()))
            .await;
    }

    // Validate Vol. 2
    {
        assert_matches!(
            harness
                .rebac_repo
                .get_entity_properties(&dataset_entity)
                .await
                .as_deref(),
            Ok([])
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MultiTenantRebacDatasetLifecycleMessageConsumerHarness {
    catalog: Catalog,
    rebac_repo: Arc<dyn RebacRepository>,
    rebac_service: Arc<dyn RebacService>,
}

impl MultiTenantRebacDatasetLifecycleMessageConsumerHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder
            .add::<MultiTenantRebacDatasetLifecycleMessageConsumer>()
            .add::<RebacServiceImpl>()
            .add_value(kamu_auth_rebac_services::DefaultAccountProperties { is_admin: false })
            .add_value(kamu_auth_rebac_services::DefaultDatasetProperties {
                allows_anonymous_read: false,
                allows_public_read: false,
            })
            .add::<InMemoryRebacRepository>();

        let catalog = catalog_builder.build();

        Self {
            rebac_repo: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub async fn mimic<TMessage: Message + 'static>(&self, message: TMessage) {
        let content_json = serde_json::to_string(&message).unwrap();

        consume_deserialized_message::<TMessage>(
            &self.catalog,
            ConsumerFilter::AllConsumers,
            &content_json,
            TMessage::version(),
        )
        .await
        .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
