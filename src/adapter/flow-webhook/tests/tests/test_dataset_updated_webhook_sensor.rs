// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;

use chrono::Utc;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_flow_webhook::*;
use kamu_flow_system::*;
use kamu_webhooks::*;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_basic_crud() {
    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let sensor = DatasetUpdatedWebhookSensorHarness::create_sensor(
        subscription_id,
        &event_type,
        &dataset_id,
    );

    let flow_scope = sensor.flow_scope();
    assert_eq!(
        flow_scope.scope_type(),
        FLOW_SCOPE_TYPE_WEBHOOK_SUBSCRIPTION
    );
    let flow_scope_webhook = FlowScopeSubscription::new(flow_scope);
    assert_eq!(flow_scope_webhook.subscription_id(), subscription_id);
    assert_eq!(flow_scope_webhook.event_type(), event_type);
    assert_eq!(
        flow_scope_webhook.maybe_dataset_id().as_ref(),
        Some(&dataset_id)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activate() {
    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let sensor = DatasetUpdatedWebhookSensorHarness::create_sensor(
        subscription_id,
        &event_type,
        &dataset_id,
    );

    let harness = DatasetUpdatedWebhookSensorHarness::new(Default::default());

    // Activation of webhooks sensor causes no actions
    sensor
        .on_activated(&harness.catalog, Utc::now())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitize_wrong_dataset() {
    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let sensor = DatasetUpdatedWebhookSensorHarness::create_sensor(
        subscription_id,
        &event_type,
        &dataset_id,
    );

    let harness = DatasetUpdatedWebhookSensorHarness::new(Default::default());

    let input_dataset_id = odf::DatasetID::new_seeded_ed25519(b"bar");
    let input_flow_binding = ingest_dataset_binding(&input_dataset_id);

    let res = sensor
        .on_sensitized(
            &harness.catalog,
            &input_flow_binding,
            &FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                activation_time: Utc::now(),
            }),
        )
        .await;
    assert_matches!(
        res,
        Err(FlowSensorSensitizationError::InvalidInputFlowBinding { binding }) if binding == input_flow_binding
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitization_got_new_data() {
    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_flow_run_service = MockFlowRunService::new();
    DatasetUpdatedWebhookSensorHarness::add_delivery_flow_expectation(
        &mut mock_flow_run_service,
        subscription_id,
        &event_type,
        &dataset_id,
    );

    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            mock_flow_run_service: Some(mock_flow_run_service),
        });

    let sensor = DatasetUpdatedWebhookSensorHarness::create_sensor(
        subscription_id,
        &event_type,
        &dataset_id,
    );

    sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&dataset_id),
            &DatasetUpdatedWebhookSensorHarness::make_input_activation_cause(
                &dataset_id,
                ResourceChanges::NewData(ResourceDataChanges {
                    blocks_added: 1,
                    records_added: 5,
                    new_watermark: None,
                }),
            ),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitization_breaking_change_similarly() {
    let subscription_id = WebhookSubscriptionID::new(Uuid::new_v4());
    let event_type = WebhookEventTypeCatalog::dataset_ref_updated();
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let mut mock_flow_run_service = MockFlowRunService::new();
    DatasetUpdatedWebhookSensorHarness::add_delivery_flow_expectation(
        &mut mock_flow_run_service,
        subscription_id,
        &event_type,
        &dataset_id,
    );

    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            mock_flow_run_service: Some(mock_flow_run_service),
        });

    let sensor = DatasetUpdatedWebhookSensorHarness::create_sensor(
        subscription_id,
        &event_type,
        &dataset_id,
    );

    sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&dataset_id),
            &DatasetUpdatedWebhookSensorHarness::make_input_activation_cause(
                &dataset_id,
                ResourceChanges::Breaking,
            ),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DatasetUpdatedWebhookSensorHarness {
    catalog: dill::Catalog,
}

#[derive(Default)]
struct DatasetUpdatedWebhookSensorHarnessOverrides {
    mock_flow_run_service: Option<MockFlowRunService>,
}

impl DatasetUpdatedWebhookSensorHarness {
    fn new(overrides: DatasetUpdatedWebhookSensorHarnessOverrides) -> Self {
        let mock_flow_run_service = overrides.mock_flow_run_service.unwrap_or_default();

        let mut b = dill::CatalogBuilder::new();
        b.add_value(mock_flow_run_service);
        b.bind::<dyn FlowRunService, MockFlowRunService>();

        let catalog = b.build();
        Self { catalog }
    }

    fn create_sensor(
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        dataset_id: &odf::DatasetID,
    ) -> DatasetUpdatedWebhookSensor {
        let webhook_flow_scope =
            FlowScopeSubscription::make_scope(subscription_id, event_type, Some(dataset_id));
        let reactive_rule = ReactiveRule::new(BatchingRule::Immediate, BreakingChangeRule::Recover);

        DatasetUpdatedWebhookSensor::new(webhook_flow_scope, reactive_rule)
    }

    fn make_input_activation_cause(
        dataset_id: &odf::DatasetID,
        changes: ResourceChanges,
    ) -> FlowActivationCause {
        FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
            activation_time: Utc::now(),
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            changes,
            details: serde_json::to_value(DatasetResourceUpdateDetails {
                dataset_id: dataset_id.clone(),
                source: DatasetUpdateSource::UpstreamFlow {
                    flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
                    flow_id: FlowID::new(1),
                    maybe_flow_config_snapshot: None,
                },
                new_head: odf::Multihash::from_digest_sha3_256(b"new_head"),
                old_head_maybe: Some(odf::Multihash::from_digest_sha3_256(b"old_head")),
            })
            .unwrap(),
        })
    }

    fn add_delivery_flow_expectation(
        mock_flow_run_service: &mut MockFlowRunService,
        subscription_id: WebhookSubscriptionID,
        event_type: &WebhookEventType,
        dataset_id: &odf::DatasetID,
    ) {
        let dataset_id_clone_1 = dataset_id.clone();
        let dataset_id_clone_2 = dataset_id.clone();

        let event_type_clone_1 = event_type.clone();
        let event_type_clone_2 = event_type.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(
                move |_,
                      flow_binding: &kamu_flow_system::FlowBinding,
                      _,
                      maybe_flow_trigger_rule,
                      maybe_forced_flow_config_rule| {
                    assert_eq!(flow_binding.flow_type, FLOW_TYPE_WEBHOOK_DELIVER);

                    assert_matches!(maybe_flow_trigger_rule, Some(FlowTriggerRule::Reactive(_)));
                    assert!(maybe_forced_flow_config_rule.is_none());

                    let webhook_scope = FlowScopeSubscription::new(&flow_binding.scope);
                    webhook_scope.subscription_id() == subscription_id
                        && webhook_scope.event_type() == event_type_clone_1
                        && webhook_scope
                            .maybe_dataset_id()
                            .as_ref()
                            .is_some_and(|id| id == &dataset_id_clone_1)
                },
            )
            .returning(move |_, _, _, _, _| {
                let now = Utc::now();

                Ok(FlowState {
                    flow_id: FlowID::new(1),
                    flow_binding: webhook_deliver_binding(
                        subscription_id,
                        &event_type_clone_2,
                        Some(&dataset_id_clone_2),
                    ),
                    start_condition: None,
                    task_ids: vec![],
                    activation_causes: vec![
                        // wrong, but irrelevant for the tests
                        FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                            activation_time: now,
                        }),
                    ],
                    late_activation_causes: vec![],
                    outcome: None,
                    timing: FlowTimingRecords {
                        first_scheduled_at: Some(now),
                        scheduled_for_activation_at: Some(now),
                        running_since: None,
                        awaiting_executor_since: None,
                        last_attempt_finished_at: None,
                        completed_at: None,
                    },
                    config_snapshot: None,
                    retry_policy: None,
                })
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
