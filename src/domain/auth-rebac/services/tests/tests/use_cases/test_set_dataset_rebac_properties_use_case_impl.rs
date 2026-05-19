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
    DatasetProperties,
    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE,
    RebacDatasetPropertiesMessage,
    RebacService,
    SetDatasetRebacPropertiesUseCase,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use messaging_outbox::{MockOutbox, Outbox};
use pretty_assertions::assert_matches;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type Harness = SetDatasetRebacPropertiesUseCaseImplHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_idempotent_properties_emits_only_once() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;
    let properties = DatasetProperties {
        allows_anonymous_read: false,
        allows_public_read: true,
    };

    let harness =
        Harness::new_with_expected_modified_message(dataset_id.clone(), properties.clone());

    assert_matches!(
        harness
            .use_case
            .execute(&dataset_id, properties.clone())
            .await,
        Ok(())
    );
    assert_matches!(
        harness.use_case.execute(&dataset_id, properties).await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_partial_change_emits_once() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let initial_properties = DatasetProperties {
        allows_anonymous_read: false,
        allows_public_read: true,
    };
    let updated_properties = DatasetProperties {
        allows_anonymous_read: true,
        allows_public_read: true,
    };

    let harness =
        Harness::new_with_expected_modified_message(dataset_id.clone(), updated_properties.clone());

    let _ = harness
        .rebac_service
        .set_dataset_property(
            &dataset_id,
            kamu_auth_rebac::DatasetPropertyName::AllowsPublicRead,
            &kamu_auth_rebac::boolean_property_value(initial_properties.allows_public_read),
        )
        .await
        .unwrap();
    assert_matches!(
        harness
            .use_case
            .execute(&dataset_id, updated_properties)
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_set_default_properties_on_new_dataset_does_not_emit_message() {
    let dataset_id = odf::DatasetID::new_generated_ed25519().1;

    let harness = Harness::new(MockOutbox::new());

    assert_matches!(
        harness
            .use_case
            .execute(&dataset_id, DatasetProperties::default())
            .await,
        Ok(())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SetDatasetRebacPropertiesUseCaseImplHarness {
    use_case: Arc<dyn SetDatasetRebacPropertiesUseCase>,
    rebac_service: Arc<dyn RebacService>,
}

impl SetDatasetRebacPropertiesUseCaseImplHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_value(mock_outbox).bind::<dyn Outbox, MockOutbox>();
            b.add::<SetDatasetRebacPropertiesUseCaseImpl>();
            b.add::<RebacServiceImpl>();
            b.add_value(DefaultAccountProperties::default());
            b.add_value(DefaultDatasetProperties::default());
            b.add::<InMemoryRebacRepository>();

            b.build()
        };

        Self {
            use_case: catalog.get_one().unwrap(),
            rebac_service: catalog.get_one().unwrap(),
        }
    }

    fn new_with_expected_modified_message(
        dataset_id: odf::DatasetID,
        expected_properties: DatasetProperties,
    ) -> Self {
        let mut outbox = MockOutbox::new();
        outbox.expect_post_message_as_json().times(1).returning(
            move |producer_name, message_as_json, version| {
                assert_eq!(
                    producer_name,
                    MESSAGE_PRODUCER_KAMU_REBAC_DATASET_PROPERTIES_SERVICE
                );
                assert_eq!(version, 1);
                assert!(matches!(
                    serde_json::from_value::<RebacDatasetPropertiesMessage>(message_as_json.clone()),
                    Ok(RebacDatasetPropertiesMessage::Modified(message))
                        if message.dataset_id == dataset_id
                            && message.dataset_properties == expected_properties
                ));
                Ok(())
            },
        );

        Self::new(outbox)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
