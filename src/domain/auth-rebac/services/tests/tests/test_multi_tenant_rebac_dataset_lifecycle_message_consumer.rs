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
use kamu_auth_rebac::{PropertyName, RebacService};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{MultiTenantRebacDatasetLifecycleMessageConsumer, RebacServiceImpl};
use kamu_core::{DatasetLifecycleMessage, DatasetVisibility};
use messaging_outbox::{consume_deserialized_message, ConsumerFilter, Message};
use opendatafabric::{AccountID, DatasetID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = MultiTenantRebacDatasetLifecycleMessageConsumerHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rebac_properties_added() {
    let harness = Harness::new();

    let (_, dataset_id_1) = DatasetID::new_generated_ed25519();
    let (_, dataset_id_2) = DatasetID::new_generated_ed25519();
    let (_, owner_id) = AccountID::new_generated_ed25519();

    // Pre-checks
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id_1)
                .await,
            Ok(props)
                if props.is_empty()
        );
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id_2)
                .await,
            Ok(props)
                if props.is_empty()
        );
    }

    // Simulate creations
    {
        harness
            .consume_message(DatasetLifecycleMessage::created(
                dataset_id_1.clone(),
                owner_id.clone(),
                DatasetVisibility::Public,
            ))
            .await;
        harness
            .consume_message(DatasetLifecycleMessage::created(
                dataset_id_2.clone(),
                owner_id,
                DatasetVisibility::Private,
            ))
            .await;
    }

    // Validate properties
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id_1)
                .await,
            Ok(props)
                if props == vec![PropertyName::dataset_allows_public_read(true)]
        );
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id_2)
                .await,
            Ok(props)
                if props == vec![PropertyName::dataset_allows_public_read(false)]
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_rebac_properties_deleted() {
    let harness = Harness::new();

    let (_, dataset_id) = DatasetID::new_generated_ed25519();
    let (_, owner_id) = AccountID::new_generated_ed25519();

    // Simulate creation
    {
        harness
            .consume_message(DatasetLifecycleMessage::created(
                dataset_id.clone(),
                owner_id.clone(),
                DatasetVisibility::Public,
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
            Ok(props)
                if props == vec![PropertyName::dataset_allows_public_read(true)]
        );
    }

    // Simulate deletion
    {
        harness
            .consume_message(DatasetLifecycleMessage::deleted(dataset_id.clone()))
            .await;
    }

    // Validate
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id)
                .await,
            Ok(props)
                if props.is_empty()
        );
    }

    // Simulate deletion again to check idempotency
    {
        harness
            .consume_message(DatasetLifecycleMessage::deleted(dataset_id.clone()))
            .await;
    }

    // Validate Vol. 2
    {
        assert_matches!(
            harness
                .rebac_service
                .get_dataset_properties(&dataset_id)
                .await,
            Ok(props)
                if props.is_empty()
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct MultiTenantRebacDatasetLifecycleMessageConsumerHarness {
    catalog: Catalog,
    rebac_service: Arc<dyn RebacService>,
}

impl MultiTenantRebacDatasetLifecycleMessageConsumerHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder
            .add::<MultiTenantRebacDatasetLifecycleMessageConsumer>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>();

        let catalog = catalog_builder.build();

        Self {
            rebac_service: catalog.get_one().unwrap(),
            catalog,
        }
    }

    pub async fn consume_message<TMessage: Message + 'static>(&self, message: TMessage) {
        let content_json = serde_json::to_string(&message).unwrap();

        consume_deserialized_message::<TMessage>(
            &self.catalog,
            ConsumerFilter::AllConsumers,
            &content_json,
        )
        .await
        .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
