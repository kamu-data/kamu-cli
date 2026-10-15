// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;

use chrono::Utc;
use kamu_adapter_flow_dataset::*;
use kamu_adapter_flow_webhook::*;
use kamu_datasets::{
    DatasetIncrementQueryService,
    DatasetReferenceService,
    MockDatasetIncrementQueryService,
};
use kamu_datasets_services::testing::FakeDatasetReferenceService;
use kamu_flow_system::*;
use kamu_webhooks::*;
use uuid::Uuid;

use super::utils::{LinearDatasetHistory, WebhookDeliveryHistory};

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
async fn test_sensor_activation_without_delivery_base_does_nothing() {
    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            delivery_base: None,
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_with_receiver_up_to_date_does_nothing() {
    let head = LinearDatasetHistory::head(1);
    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            delivery_base: Some(head.clone()),
            dataset_head: Some(head),
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_for_dataset_without_head_does_nothing() {
    // A deleted dataset loses its head, while its subscription's trigger remains
    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            delivery_base: Some(LinearDatasetHistory::head(1)),
            dataset_head: None,
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_catches_up_new_data() {
    let (head_1, head_3) = (LinearDatasetHistory::head(1), LinearDatasetHistory::head(3));

    let mut mock_flow_run_service = MockFlowRunService::new();
    DatasetUpdatedWebhookSensorHarness::expect_delivery_flow_for_update(
        &mut mock_flow_run_service,
        &head_1,
        &head_3,
        ExpectedChanges::NewData,
    );

    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            mock_flow_run_service: Some(mock_flow_run_service),
            delivery_base: Some(head_1),
            dataset_head: Some(head_3),
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_catches_up_breaking_change() {
    let head_1 = LinearDatasetHistory::head(1);
    let diverged_head = LinearDatasetHistory::diverged_head();

    let mut mock_flow_run_service = MockFlowRunService::new();
    DatasetUpdatedWebhookSensorHarness::expect_delivery_flow_for_update(
        &mut mock_flow_run_service,
        &head_1,
        &diverged_head,
        ExpectedChanges::Breaking,
    );

    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            mock_flow_run_service: Some(mock_flow_run_service),
            delivery_base: Some(head_1),
            dataset_head: Some(diverged_head),
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_skips_breaking_change_without_recovery() {
    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            delivery_base: Some(LinearDatasetHistory::head(1)),
            dataset_head: Some(LinearDatasetHistory::diverged_head()),
            for_breaking_change: Some(BreakingChangeRule::NoAction),
            ..Default::default()
        })
        .await;

    harness.activate_sensor().await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_skips_updates_delivered_by_catch_up() {
    let [head_1, head_2, head_3, head_4] = [1, 2, 3, 4].map(LinearDatasetHistory::head);

    let mut mock_flow_run_service = MockFlowRunService::new();
    DatasetUpdatedWebhookSensorHarness::expect_delivery_flow_for_update(
        &mut mock_flow_run_service,
        &head_1,
        &head_3,
        ExpectedChanges::NewData,
    );
    DatasetUpdatedWebhookSensorHarness::expect_delivery_flow_for_update(
        &mut mock_flow_run_service,
        &head_3,
        &head_4,
        ExpectedChanges::NewData,
    );

    let harness =
        DatasetUpdatedWebhookSensorHarness::new(DatasetUpdatedWebhookSensorHarnessOverrides {
            mock_flow_run_service: Some(mock_flow_run_service),
            delivery_base: Some(head_1.clone()),
            dataset_head: Some(head_3.clone()),
            ..Default::default()
        })
        .await;

    // Catches up from 1 to 3, then the updates to 2 and 3 are dispatched late
    harness.activate_sensor().await;
    harness.sensitize_sensor(&head_1, &head_2).await;
    harness.sensitize_sensor(&head_2, &head_3).await;

    // An update past the caught-up head is delivered
    harness.sensitize_sensor(&head_3, &head_4).await;
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

    let harness = DatasetUpdatedWebhookSensorHarness::new(Default::default()).await;

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
            ..Default::default()
        })
        .await;

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
            ..Default::default()
        })
        .await;

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
    sensor: DatasetUpdatedWebhookSensor,
}

#[derive(Default)]
struct DatasetUpdatedWebhookSensorHarnessOverrides {
    mock_flow_run_service: Option<MockFlowRunService>,
    /// Head the receiver already knows
    delivery_base: Option<odf::Multihash>,
    /// Current head of the dataset
    dataset_head: Option<odf::Multihash>,
    /// Rule of the sensor, `Recover` by default
    for_breaking_change: Option<BreakingChangeRule>,
}

enum ExpectedChanges {
    NewData,
    Breaking,
}

impl DatasetUpdatedWebhookSensorHarness {
    async fn new(overrides: DatasetUpdatedWebhookSensorHarnessOverrides) -> Self {
        let mock_flow_run_service = overrides.mock_flow_run_service.unwrap_or_default();

        let dataset_references = FakeDatasetReferenceService::new();
        if let Some(dataset_head) = &overrides.dataset_head {
            dataset_references.set_head(&Self::dataset_id(), dataset_head);
        }

        let mut b = dill::CatalogBuilder::new();
        b.add_value(mock_flow_run_service);
        b.bind::<dyn FlowRunService, MockFlowRunService>();
        WebhookDeliveryHistory::register(&mut b);
        b.add_value(dataset_references);
        b.bind::<dyn DatasetReferenceService, FakeDatasetReferenceService>();
        b.add_value(LinearDatasetHistory::increments());
        b.bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>();

        let catalog = b.build();

        // With no successful deliveries, the receiver knows the head at creation
        let webhook_flow_scope = FlowScopeSubscription::make_scope(
            Self::subscription_id(),
            &WebhookEventTypeCatalog::dataset_ref_updated(),
            Some(&Self::dataset_id()),
        );
        WebhookDeliveryHistory::from_catalog(&catalog)
            .create_subscription(&webhook_flow_scope, overrides.delivery_base.as_ref())
            .await;

        let sensor = DatasetUpdatedWebhookSensor::new(
            webhook_flow_scope,
            ReactiveRule::new(
                BatchingRule::Immediate,
                overrides
                    .for_breaking_change
                    .unwrap_or(BreakingChangeRule::Recover),
            ),
        );

        Self { catalog, sensor }
    }

    fn subscription_id() -> WebhookSubscriptionID {
        WebhookSubscriptionID::new(Uuid::from_u128(1))
    }

    fn dataset_id() -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(b"foo")
    }

    async fn activate_sensor(&self) {
        self.sensor
            .on_activated(&self.catalog, Utc::now())
            .await
            .unwrap();
    }

    /// Dispatches a dataset update to the sensor
    async fn sensitize_sensor(&self, old_head: &odf::Multihash, new_head: &odf::Multihash) {
        self.sensor
            .on_sensitized(
                &self.catalog,
                &ingest_dataset_binding(&Self::dataset_id()),
                &FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
                    activation_time: Utc::now(),
                    resource_type: DATASET_RESOURCE_TYPE.to_string(),
                    changes: ResourceChanges::NewData(ResourceDataChanges {
                        blocks_added: 1,
                        records_added: 1,
                        new_watermark: None,
                    }),
                    details: serde_json::to_value(DatasetResourceUpdateDetails {
                        dataset_id: Self::dataset_id(),
                        source: DatasetUpdateSource::HttpIngest { source_name: None },
                        new_head: new_head.clone(),
                        old_head_maybe: Some(old_head.clone()),
                    })
                    .unwrap(),
                }),
            )
            .await
            .unwrap();
    }

    /// Expects one delivery flow activated by an update between the given heads
    fn expect_delivery_flow_for_update(
        mock_flow_run_service: &mut MockFlowRunService,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
        expected_changes: ExpectedChanges,
    ) {
        let old_head = old_head.clone();
        let new_head = new_head.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(move |_, flow_binding, activation_causes, _, _| {
                let [FlowActivationCause::ResourceUpdate(update)] = activation_causes.as_slice()
                else {
                    return false;
                };
                let details =
                    serde_json::from_value::<DatasetResourceUpdateDetails>(update.details.clone())
                        .unwrap();
                let changes_match = match (&expected_changes, &update.changes) {
                    (ExpectedChanges::NewData, ResourceChanges::NewData(_))
                    | (ExpectedChanges::Breaking, ResourceChanges::Breaking) => true,
                    (ExpectedChanges::NewData, ResourceChanges::Breaking)
                    | (ExpectedChanges::Breaking, ResourceChanges::NewData(_)) => false,
                };
                flow_binding.flow_type == FLOW_TYPE_WEBHOOK_DELIVER
                    && details.old_head_maybe.as_ref() == Some(&old_head)
                    && details.new_head == new_head
                    && changes_match
            })
            .times(1)
            .returning(|_, flow_binding, _, _, _| Ok(Self::flow_state(flow_binding.clone())));
    }

    fn flow_state(flow_binding: FlowBinding) -> FlowState {
        let now = Utc::now();
        FlowState {
            flow_id: FlowID::new(1),
            flow_binding,
            start_condition: None,
            task_ids: vec![],
            activation_causes: vec![],
            late_activation_causes: vec![],
            outcome: None,
            timing: FlowTimingRecords {
                first_activated_at: None,
                scheduled_for_activation_at: Some(now),
                running_since: None,
                awaiting_executor_since: None,
                last_attempt_finished_at: None,
                completed_at: None,
            },
            config_snapshot: None,
            retry_policy: None,
        }
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
                        first_activated_at: None,
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
