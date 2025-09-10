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

use chrono::Utc;
use kamu_adapter_flow_dataset::*;
use kamu_datasets::{
    DatasetDependenciesMessage,
    DatasetIncrementQueryService,
    DatasetIntervalIncrement,
    MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
};
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_datasets_services::testing::MockDatasetIncrementQueryService;
use kamu_flow_system::*;
use messaging_outbox::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_basic_crud() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");
    let baz_id = odf::DatasetID::new_seeded_ed25519(b"baz");
    let quix_id = odf::DatasetID::new_seeded_ed25519(b"quix");

    let harness = DerivedDatasetFlowSensorHarness::new(Default::default());
    harness
        .declare_dependency(&foo_id, &[bar_id.clone(), baz_id.clone()], &[])
        .await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);

    let flow_scope = sensor.flow_scope();
    assert_eq!(flow_scope.scope_type(), FLOW_SCOPE_TYPE_DATASET);
    assert_eq!(FlowScopeDataset::new(flow_scope).dataset_id(), foo_id);

    let scopes = sensor.get_sensitive_to_scopes(&harness.catalog).await;
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].scope_type(), FLOW_SCOPE_TYPE_DATASET);
    assert_eq!(scopes[1].scope_type(), FLOW_SCOPE_TYPE_DATASET);
    let scope_ids: std::collections::HashSet<_> = scopes
        .iter()
        .map(|scope| FlowScopeDataset::new(scope).dataset_id())
        .collect();
    assert_eq!(
        scope_ids,
        [bar_id.clone(), baz_id.clone()].into_iter().collect()
    );

    harness
        .declare_dependency(&foo_id, &[quix_id.clone(), baz_id.clone()], &[bar_id])
        .await;

    let scopes = sensor.get_sensitive_to_scopes(&harness.catalog).await;
    assert_eq!(scopes.len(), 2);
    assert_eq!(scopes[0].scope_type(), FLOW_SCOPE_TYPE_DATASET);
    assert_eq!(scopes[1].scope_type(), FLOW_SCOPE_TYPE_DATASET);
    let scope_ids: std::collections::HashSet<_> = scopes
        .iter()
        .map(|scope| FlowScopeDataset::new(scope).dataset_id())
        .collect();
    assert_eq!(scope_ids, [baz_id, quix_id].into_iter().collect());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_up_to_date() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mock_transform_flow_evaluator = {
        let foo_id = foo_id.clone();
        let mut mock = MockTransformFlowEvaluator::new();
        mock.expect_evaluate_transform_status()
            .withf(move |dataset_id| dataset_id == &foo_id)
            .returning(|_| Ok(kamu_core::TransformStatus::UpToDate));
        mock
    };

    let harness = DerivedDatasetFlowSensorHarness::new(DerivedDatasetFlowSensorHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        ..Default::default()
    });
    harness.declare_dependency(&foo_id, &[bar_id], &[]).await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);

    sensor
        .on_activated(&harness.catalog, Utc::now())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_activation_new_data() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mock_transform_flow_evaluator = {
        let foo_id = foo_id.clone();
        let bar_id = bar_id.clone();

        let mut mock = MockTransformFlowEvaluator::new();
        mock.expect_evaluate_transform_status()
            .withf(move |dataset_id| dataset_id == &foo_id)
            .returning(move |_| {
                Ok(kamu_core::TransformStatus::NewInputDataAvailable {
                    input_advancements: vec![odf::metadata::ExecuteTransformInput {
                        dataset_id: bar_id.clone(),
                        prev_block_hash: Some(odf::Multihash::from_digest_sha3_256(
                            b"bar_prev_slice",
                        )),
                        new_block_hash: Some(odf::Multihash::from_digest_sha3_256(
                            b"bar_new_slice",
                        )),
                        prev_offset: Some(5),
                        new_offset: Some(10),
                    }],
                })
            });
        mock
    };

    let mut mock_flow_run_service = MockFlowRunService::new();
    DerivedDatasetFlowSensorHarness::add_transform_flow_trigger_expectation(
        &mut mock_flow_run_service,
        &foo_id,
    );

    let mock_dataset_increment_query_service =
        MockDatasetIncrementQueryService::with_increment_between_for_args(
            bar_id.clone(),
            Some(odf::Multihash::from_digest_sha3_256(b"bar_prev_slice")),
            odf::Multihash::from_digest_sha3_256(b"bar_new_slice"),
            DatasetIntervalIncrement {
                num_blocks: 1,
                num_records: 5,
                updated_watermark: None,
            },
        );

    let harness = DerivedDatasetFlowSensorHarness::new(DerivedDatasetFlowSensorHarnessOverrides {
        mock_transform_flow_evaluator: Some(mock_transform_flow_evaluator),
        mock_flow_run_service: Some(mock_flow_run_service),
        mock_dataset_increment_query_service: Some(mock_dataset_increment_query_service),
    });
    harness.declare_dependency(&foo_id, &[bar_id], &[]).await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);

    sensor
        .on_activated(&harness.catalog, Utc::now())
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitization_wrong_dataset() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");
    let baz_id = odf::DatasetID::new_seeded_ed25519(b"baz");

    let harness = DerivedDatasetFlowSensorHarness::new(Default::default());
    harness.declare_dependency(&foo_id, &[bar_id], &[]).await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);
    let res = sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&baz_id),
            // Wrong cause, but irrelevant for this test
            &FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
                activation_time: Utc::now(),
            }),
        )
        .await;
    assert_matches!(
        res,
        Err(FlowSensorSensitizationError::InvalidInputFlowBinding { binding })
            if binding == ingest_dataset_binding(&baz_id)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitization_got_new_data() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mut mock_flow_run_service = MockFlowRunService::new();
    DerivedDatasetFlowSensorHarness::add_transform_flow_trigger_expectation(
        &mut mock_flow_run_service,
        &foo_id,
    );

    let harness = DerivedDatasetFlowSensorHarness::new(DerivedDatasetFlowSensorHarnessOverrides {
        mock_flow_run_service: Some(mock_flow_run_service),
        ..Default::default()
    });
    harness
        .declare_dependency(&foo_id, &[bar_id.clone()], &[])
        .await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);

    sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&bar_id),
            &DerivedDatasetFlowSensorHarness::make_input_activation_cause(
                &bar_id,
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
async fn test_sensor_sensitization_breaking_change_ignored() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let harness = DerivedDatasetFlowSensorHarness::new(Default::default());
    harness
        .declare_dependency(&foo_id, &[bar_id.clone()], &[])
        .await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor(&foo_id);

    sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&bar_id),
            // This breaking change should be ignored, as default reactive rule is NoAction
            &DerivedDatasetFlowSensorHarness::make_input_activation_cause(
                &bar_id,
                ResourceChanges::Breaking,
            ),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_sensor_sensitization_breaking_change_recovered() {
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let mut mock_flow_run_service = MockFlowRunService::new();
    DerivedDatasetFlowSensorHarness::add_metadata_only_reset_trigger_expectation(
        &mut mock_flow_run_service,
        &foo_id,
    );

    let harness = DerivedDatasetFlowSensorHarness::new(DerivedDatasetFlowSensorHarnessOverrides {
        mock_flow_run_service: Some(mock_flow_run_service),
        ..Default::default()
    });
    harness
        .declare_dependency(&foo_id, &[bar_id.clone()], &[])
        .await;

    let sensor = DerivedDatasetFlowSensorHarness::create_sensor_with_reactive_rule(
        &foo_id,
        ReactiveRule {
            for_new_data: BatchingRule::Immediate,
            for_breaking_change: BreakingChangeRule::Recover,
        },
    );

    sensor
        .on_sensitized(
            &harness.catalog,
            &ingest_dataset_binding(&bar_id),
            &DerivedDatasetFlowSensorHarness::make_input_activation_cause(
                &bar_id,
                ResourceChanges::Breaking,
            ),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct DerivedDatasetFlowSensorHarness {
    catalog: dill::Catalog,
    outbox: Arc<dyn Outbox>,
}

#[derive(Default)]
struct DerivedDatasetFlowSensorHarnessOverrides {
    mock_transform_flow_evaluator: Option<MockTransformFlowEvaluator>,
    mock_flow_run_service: Option<MockFlowRunService>,
    mock_dataset_increment_query_service: Option<MockDatasetIncrementQueryService>,
}

impl DerivedDatasetFlowSensorHarness {
    fn new(overrides: DerivedDatasetFlowSensorHarnessOverrides) -> Self {
        let mock_transform_flow_evaluator =
            overrides.mock_transform_flow_evaluator.unwrap_or_default();
        let mock_flow_run_service = overrides.mock_flow_run_service.unwrap_or_default();
        let mock_dataset_increment_query_service = overrides
            .mock_dataset_increment_query_service
            .unwrap_or_default();

        use dill::Component;

        let mut b = dill::CatalogBuilder::new();
        b.add_value(mock_transform_flow_evaluator);
        b.bind::<dyn TransformFlowEvaluator, MockTransformFlowEvaluator>();
        b.add_value(mock_flow_run_service);
        b.bind::<dyn FlowRunService, MockFlowRunService>();
        b.add_value(mock_dataset_increment_query_service);
        b.bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>();
        b.add::<DependencyGraphServiceImpl>();
        b.add_builder(
            OutboxImmediateImpl::builder().with_consumer_filter(ConsumerFilter::AllConsumers),
        );
        b.bind::<dyn Outbox, OutboxImmediateImpl>();

        register_message_dispatcher::<DatasetDependenciesMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
        );

        let catalog = b.build();
        Self {
            outbox: catalog.get_one().unwrap(),
            catalog,
        }
    }

    async fn declare_dependency(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        added_upstream_ids: &[odf::DatasetID],
        removed_upstream_ids: &[odf::DatasetID],
    ) {
        let message = DatasetDependenciesMessage::updated(
            downstream_dataset_id,
            added_upstream_ids.to_vec(),
            removed_upstream_ids.to_vec(),
        );
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                message,
            )
            .await
            .unwrap();
    }

    fn create_sensor(dataset_id: &odf::DatasetID) -> DerivedDatasetFlowSensor {
        Self::create_sensor_with_reactive_rule(dataset_id, ReactiveRule::empty())
    }

    fn create_sensor_with_reactive_rule(
        dataset_id: &odf::DatasetID,
        reactive_rule: ReactiveRule,
    ) -> DerivedDatasetFlowSensor {
        DerivedDatasetFlowSensor::new(dataset_id, reactive_rule)
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
                new_head: odf::Multihash::from_digest_sha3_256(b"new_bar_head"),
                old_head_maybe: Some(odf::Multihash::from_digest_sha3_256(b"old_bar_head")),
            })
            .unwrap(),
        })
    }

    fn add_transform_flow_trigger_expectation(
        mock_flow_run_service: &mut MockFlowRunService,
        dataset_id: &odf::DatasetID,
    ) {
        let dataset_id_clone_1 = dataset_id.clone();
        let dataset_id_clone_2 = dataset_id.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(
                move |flow_binding: &kamu_flow_system::FlowBinding,
                      _,
                      maybe_flow_trigger_rule,
                      maybe_forced_flow_config_rule| {
                    assert_eq!(flow_binding.flow_type, FLOW_TYPE_DATASET_TRANSFORM);

                    assert!(maybe_flow_trigger_rule.as_ref().is_some_and(
                        |rule| *rule == FlowTriggerRule::Reactive(ReactiveRule::empty())
                    ));
                    assert!(maybe_forced_flow_config_rule.is_none());

                    let dataset_scope = FlowScopeDataset::new(&flow_binding.scope);
                    dataset_scope.dataset_id() == dataset_id_clone_1
                },
            )
            .returning(move |_, _, _, _| {
                let now = Utc::now();

                Ok(FlowState {
                    flow_id: FlowID::new(1),
                    flow_binding: transform_dataset_binding(&dataset_id_clone_2),
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
                    },
                    config_snapshot: None,
                    retry_policy: None,
                })
            });
    }

    fn add_metadata_only_reset_trigger_expectation(
        mock_flow_run_service: &mut MockFlowRunService,
        dataset_id: &odf::DatasetID,
    ) {
        let dataset_id_clone_1 = dataset_id.clone();
        let dataset_id_clone_2 = dataset_id.clone();

        mock_flow_run_service
            .expect_run_flow_automatically()
            .withf(
                move |flow_binding: &kamu_flow_system::FlowBinding,
                      _,
                      maybe_flow_trigger_rule,
                      maybe_forced_flow_config_rule| {
                    assert_eq!(flow_binding.flow_type, FLOW_TYPE_DATASET_RESET_TO_METADATA);

                    assert!(maybe_flow_trigger_rule.is_none());
                    assert!(maybe_forced_flow_config_rule.is_none());

                    let dataset_scope = FlowScopeDataset::new(&flow_binding.scope);
                    dataset_scope.dataset_id() == dataset_id_clone_1
                },
            )
            .returning(move |_, _, _, _| {
                let now = Utc::now();

                Ok(FlowState {
                    flow_id: FlowID::new(1),
                    flow_binding: reset_to_metadata_dataset_binding(&dataset_id_clone_2),
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
                    },
                    config_snapshot: None,
                    retry_policy: None,
                })
            });
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
