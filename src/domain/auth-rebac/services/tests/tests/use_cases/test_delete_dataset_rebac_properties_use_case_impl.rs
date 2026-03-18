// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::CatalogBuilder;
use kamu_auth_rebac::{
    DatasetPropertyName,
    DeleteDatasetRebacPropertiesUseCase,
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
    RebacDatasetPropertiesMessage,
    RebacService,
    boolean_property_value,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use messaging_outbox::{MockOutbox, Outbox};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = DeleteDatasetRebacPropertiesUseCaseImplHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_existing_properties_emits_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let harness = Harness::new_with_expected_deleted_message(dataset_id.clone());

    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            DatasetPropertyName::AllowsPublicRead,
            &boolean_property_value(true),
        )
        .await
        .unwrap();

    assert_matches!(harness.delete_use_case.execute(&dataset_id).await, Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_delete_missing_properties_does_not_emit_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let harness = Harness::new(MockOutbox::new());

    assert_matches!(harness.delete_use_case.execute(&dataset_id).await, Ok(()));
    assert_matches!(harness.delete_use_case.execute(&dataset_id).await, Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DeleteDatasetRebacPropertiesUseCaseImplHarness {
    delete_use_case: Arc<dyn DeleteDatasetRebacPropertiesUseCase>,
    rebac_service: Arc<dyn RebacService>,
}

impl DeleteDatasetRebacPropertiesUseCaseImplHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
            b.add::<DeleteDatasetRebacPropertiesUseCaseImpl>();
            b.add::<RebacServiceImpl>();
            b.add_value(DefaultAccountProperties::default());
            b.add_value(DefaultDatasetProperties::default());
            b.add::<InMemoryRebacRepository>();

            b.build()
        };

        Self {
            delete_use_case: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
        }
    }

    fn new_with_expected_deleted_message(dataset_id: odf::DatasetID) -> Self {
        let mut outbox = MockOutbox::new();
        outbox.expect_post_message_as_json().times(1).returning(
            move |producer_name, message_as_json, version| {
                assert_eq!(
                    producer_name,
                    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE
                );
                assert_eq!(version, 1);
                let message = serde_json::from_value::<RebacDatasetPropertiesMessage>(
                    message_as_json.clone(),
                )
                .unwrap();
                assert!(
                    matches!(
                        message,
                        RebacDatasetPropertiesMessage::Deleted(ref message)
                            if message.dataset_id == dataset_id
                    ),
                    "unexpected message: {message:?}"
                );
                Ok(())
            },
        );

        Self::new(outbox)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
