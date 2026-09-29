// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use async_utils::BackgroundAgent;
use chrono::{DateTime, Duration, TimeZone, Utc};
use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use dill::*;
use internal_error::InternalError;
use kamu_accounts::DEFAULT_ACCOUNT_NAME_STR;
use kamu_adapter_flow_dataset::*;
use kamu_datasets::*;
use kamu_datasets_inmem::InMemoryDatasetDependencyRepository;
use kamu_datasets_services::DependencyGraphServiceImpl;
use kamu_datasets_services::testing::{FakeDatasetEntryService, MockDatasetIncrementQueryService};
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use kamu_task_system::{
    MESSAGE_PRODUCER_KAMU_TASK_AGENT,
    Task,
    TaskEventStore,
    TaskID,
    TaskProgressMessage,
};
use kamu_task_system_inmem::{InMemoryTaskEventStore, InMemoryTaskQueueWakeupSource};
use kamu_task_system_services::TaskSchedulerImpl;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use time_source::{FakeSystemTimeSource, SystemTimeSource};
use tokio::task::yield_now;
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

use super::{
    FlowSystemTestListener,
    ManualFlowAbortArgs,
    ManualFlowAbortDriver,
    ManualFlowActivationArgs,
    ManualFlowActivationDriver,
    TaskDriver,
    TaskDriverArgs,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const SCHEDULING_ALIGNMENT_MS: i64 = 10;
pub(crate) const SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS: i64 = SCHEDULING_ALIGNMENT_MS * 2;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct FlowHarness {
    pub catalog: Catalog,
    pub outbox: Arc<dyn Outbox>,
    pub fake_dataset_entry_service: Arc<FakeDatasetEntryService>,
    pub fake_system_time_source: FakeSystemTimeSource,
    pub dataset_entry_service: Arc<dyn DatasetEntryService>,

    pub flow_configuration_service: Arc<dyn FlowConfigurationService>,
    pub flow_trigger_service: Arc<dyn FlowTriggerService>,
    pub flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    pub flow_agent: Arc<FlowAgentImpl>,
    pub flow_system_event_agent: Arc<dyn FlowSystemEventAgent>,
    pub flow_query_service: Arc<dyn FlowQueryService>,
    pub flow_event_store: Arc<dyn FlowEventStore>,
}

#[derive(Default)]
pub(crate) struct FlowHarnessOverrides {
    pub awaiting_step: Option<Duration>,
    pub mandatory_throttling_period: Option<Duration>,
    /// Due flows loaded per page by the flow agent
    pub activation_batch_size: Option<usize>,
    pub mock_dataset_changes: Option<MockDatasetIncrementQueryService>,
    pub mock_transform_flow_evaluator: Option<MockTransformFlowEvaluator>,
    /// Registers a projector that fails on every event
    pub with_failing_projector: bool,
    /// Registers a projector that fails on its first event only
    pub with_flaky_projector: bool,
}

impl FlowHarness {
    pub fn new() -> Self {
        Self::with_overrides(FlowHarnessOverrides::default())
    }

    pub fn with_overrides(overrides: FlowHarnessOverrides) -> Self {
        let t = Utc.with_ymd_and_hms(2050, 1, 1, 12, 0, 0).unwrap();
        let fake_system_time_source = FakeSystemTimeSource::new(t);

        let awaiting_step = overrides
            .awaiting_step
            .unwrap_or(Duration::milliseconds(SCHEDULING_ALIGNMENT_MS));

        let mandatory_throttling_period =
            overrides
                .mandatory_throttling_period
                .unwrap_or(Duration::milliseconds(
                    SCHEDULING_MANDATORY_THROTTLING_PERIOD_MS,
                ));

        let mock_dataset_changes = overrides.mock_dataset_changes.unwrap_or_default();
        let mock_transform_flow_evaluator =
            overrides.mock_transform_flow_evaluator.unwrap_or_default();

        let catalog = {
            let mut b = CatalogBuilder::new();

            if overrides.with_failing_projector {
                b.add::<FailingFlowSystemEventProjector>();
            }
            if overrides.with_flaky_projector {
                b.add::<FlakyFlowSystemEventProjector>();
            }

            b.add_builder(messaging_outbox::OutboxImmediateImpl::builder(
                messaging_outbox::ConsumerFilter::AllConsumers,
            ))
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<FlowSystemTestListener>()
            .add_value(FlowAgentConfig::new(
                awaiting_step,
                mandatory_throttling_period,
                HashMap::new(),
            ))
            .add_value(FlowAgentActivationConfig {
                batch_size: NonZeroUsize::new(overrides.activation_batch_size.unwrap_or(20))
                    .unwrap(),
                concurrency: NonZeroUsize::new(8).unwrap(),
            })
            .add_value(FlowSystemEventAgentConfig { batch_size: 10 })
            .add_value(WakeupListenerConfig {
                // In-memory stores used to ignore it: keep test timings unchanged
                min_debounce_interval: std::time::Duration::ZERO,
                // Scenarios run on virtual time: wall-clock fallback timeouts must never fire,
                // or they add polls at random moments and reorder same-moment events
                max_listening_timeout: std::time::Duration::from_hours(1),
            })
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryFlowActivationWakeupSource>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>()
            .add::<InMemoryFlowProcessState>()
            .add_value(fake_system_time_source.clone())
            .bind::<dyn SystemTimeSource, FakeSystemTimeSource>()
            .add_value(mock_dataset_changes)
            .bind::<dyn DatasetIncrementQueryService, MockDatasetIncrementQueryService>()
            .add_value(mock_transform_flow_evaluator)
            .bind::<dyn TransformFlowEvaluator, MockTransformFlowEvaluator>()
            .add::<DependencyGraphServiceImpl>()
            .add::<InMemoryDatasetDependencyRepository>()
            .add::<TaskSchedulerImpl>()
            .add::<InMemoryTaskEventStore>()
            .add::<InMemoryTaskQueueWakeupSource>()
            .add::<DatabaseTransactionRunner>()
            .add::<FakeDatasetEntryService>();

            NoOpDatabasePlugin::init_database_components(&mut b);

            kamu_flow_system_services::register_dependencies(&mut b);
            kamu_adapter_flow_dataset::register_dependencies(
                &mut b,
                kamu_adapter_flow_dataset::FlowDatasetAdapterDependencyOpts {
                    with_default_transform_evaluator: false,
                },
            );

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );
            register_message_dispatcher::<DatasetDependenciesMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
            );
            register_message_dispatcher::<TaskProgressMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_TASK_AGENT,
            );
            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );
            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );

            b.build()
        };

        Self {
            outbox: catalog.get_one().unwrap(),
            fake_dataset_entry_service: catalog.get_one().unwrap(),
            dataset_entry_service: catalog.get_one().unwrap(),

            flow_agent: catalog.get_one().unwrap(),
            flow_system_event_agent: catalog.get_one().unwrap(),
            flow_query_service: catalog.get_one().unwrap(),
            flow_configuration_service: catalog.get_one().unwrap(),
            flow_trigger_service: catalog.get_one().unwrap(),
            flow_trigger_event_store: catalog.get_one().unwrap(),
            flow_event_store: catalog.get_one().unwrap(),

            fake_system_time_source,
            catalog,
        }
    }

    pub async fn create_root_dataset(&self, dataset_alias: odf::DatasetAlias) -> odf::DatasetID {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(dataset_alias.dataset_name.as_bytes());
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_alias.dataset_name.clone(),
            kind: odf::DatasetKind::Root,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    self.now(),
                    dataset_id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_alias.dataset_name,
                ),
            )
            .await
            .unwrap();

        dataset_id
    }

    pub async fn create_derived_dataset(
        &self,
        dataset_alias: odf::DatasetAlias,
        input_ids: Vec<odf::DatasetID>,
    ) -> odf::DatasetID {
        let dataset_id = odf::DatasetID::new_seeded_ed25519(dataset_alias.dataset_name.as_bytes());
        let owner_id = odf::metadata::testing::account_id_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );
        let owner_name = odf::metadata::testing::account_name_by_maybe_name(
            &dataset_alias.account_name,
            DEFAULT_ACCOUNT_NAME_STR,
        );

        self.fake_dataset_entry_service.add_entry(DatasetEntry {
            created_at: self.now(),
            id: dataset_id.clone(),
            owner_id: owner_id.clone(),
            owner_name,
            name: dataset_alias.dataset_name.clone(),
            kind: odf::DatasetKind::Derivative,
        });

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    self.now(),
                    dataset_id.clone(),
                    owner_id,
                    odf::DatasetVisibility::Public,
                    dataset_alias.dataset_name,
                ),
            )
            .await
            .unwrap();

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
                DatasetDependenciesMessage::updated(&dataset_id, input_ids, vec![]),
            )
            .await
            .unwrap();

        dataset_id
    }

    pub async fn issue_dataset_deleted(&self, dataset_id: &odf::DatasetID) {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(self.now(), dataset_id.clone()),
            )
            .await
            .unwrap();
    }

    pub async fn set_flow_trigger(
        &self,
        request_time: DateTime<Utc>,
        flow_binding: FlowBinding,
        trigger_rule: FlowTriggerRule,
        stop_policy: FlowTriggerStopPolicy,
    ) {
        self.flow_trigger_service
            .set_trigger(request_time, flow_binding, trigger_rule, stop_policy)
            .await
            .unwrap();
    }

    pub async fn get_flow_trigger_status(
        &self,
        flow_binding: &FlowBinding,
    ) -> Option<FlowTriggerStatus> {
        self.flow_trigger_service
            .find_trigger(flow_binding)
            .await
            .unwrap()
            .map(|t| t.status)
    }

    pub async fn set_dataset_flow_ingest(
        &self,
        flow_binding: FlowBinding,
        ingest_rule: FlowConfigRuleIngest,
        retry_policy: Option<RetryPolicy>,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, ingest_rule.into_flow_config(), retry_policy)
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_reset_rule(
        &self,
        flow_binding: FlowBinding,
        reset_rule: FlowConfigRuleReset,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, reset_rule.into_flow_config(), None)
            .await
            .unwrap();
    }

    pub async fn set_dataset_flow_compaction_rule(
        &self,
        flow_binding: FlowBinding,
        compaction_rule: FlowConfigRuleCompact,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, compaction_rule.into_flow_config(), None)
            .await
            .unwrap();
    }

    pub async fn pause_flow(&self, request_time: DateTime<Utc>, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .pause_flow_trigger(request_time, flow_binding)
            .await
            .unwrap();
    }

    pub async fn resume_flow(&self, request_time: DateTime<Utc>, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .resume_flow_trigger(request_time, flow_binding)
            .await
            .unwrap();
    }

    /// Stores a waiting flow scheduled for activation at the given moment,
    /// bypassing triggers and flow controllers
    pub async fn schedule_flow_for_activation(
        &self,
        flow_binding: &FlowBinding,
        activation_at: DateTime<Utc>,
    ) -> FlowID {
        let now = self.now();
        let flow_id = self.flow_event_store.new_flow_id().await.unwrap();

        self.flow_event_store
            .save_events(
                &flow_id,
                None,
                vec![
                    FlowEventInitiated {
                        event_time: now,
                        flow_id,
                        flow_binding: flow_binding.clone(),
                        activation_cause: FlowActivationCause::AutoPolling(
                            FlowActivationCauseAutoPolling {
                                activation_time: now,
                            },
                        ),
                        config_snapshot: None,
                        retry_policy: None,
                    }
                    .into(),
                    FlowEventStartConditionUpdated {
                        event_time: now,
                        flow_id,
                        flow_binding: flow_binding.clone(),
                        start_condition: FlowStartCondition::Schedule(FlowStartConditionSchedule {
                            wake_up_at: activation_at,
                        }),
                        last_activation_cause_index: 0,
                    }
                    .into(),
                    FlowEventScheduledForActivation {
                        event_time: now,
                        flow_id,
                        flow_binding: flow_binding.clone(),
                        scheduled_for_activation_at: activation_at,
                    }
                    .into(),
                ],
            )
            .await
            .unwrap();

        flow_id
    }

    pub async fn task_exists(&self, task_id: TaskID) -> bool {
        let task_event_store = self.catalog.get_one::<dyn TaskEventStore>().unwrap();
        Task::try_load(task_id, task_event_store.as_ref())
            .await
            .unwrap()
            .is_some()
    }

    pub async fn task_cancellation_requested(&self, task_id: TaskID) -> bool {
        let task_event_store = self.catalog.get_one::<dyn TaskEventStore>().unwrap();
        Task::load(task_id, task_event_store.as_ref())
            .await
            .unwrap()
            .timing
            .cancellation_requested_at
            .is_some()
    }

    pub fn flow_activations(&self, flow_type: &str, outcome: &str) -> u64 {
        self.catalog
            .get_one::<FlowAgentMetrics>()
            .unwrap()
            .activations_total
            .with_label_values(&[flow_type, outcome])
            .get()
    }

    pub fn flow_activation_delay_samples(&self) -> u64 {
        self.catalog
            .get_one::<FlowAgentMetrics>()
            .unwrap()
            .activation_delay_seconds
            .get_sample_count()
    }

    pub fn flow_activation_delays_total_seconds(&self) -> f64 {
        self.catalog
            .get_one::<FlowAgentMetrics>()
            .unwrap()
            .activation_delay_seconds
            .get_sample_sum()
    }

    pub fn completed_flows(&self, flow_type: &str, outcome: &str) -> u64 {
        self.completion_metrics()
            .flow_duration_seconds
            .with_label_values(&[flow_type, outcome])
            .get_sample_count()
    }

    pub fn assert_completed_flows_duration_seconds(
        &self,
        flow_type: &str,
        outcome: &str,
        expected: f64,
    ) {
        let actual = self
            .completion_metrics()
            .flow_duration_seconds
            .with_label_values(&[flow_type, outcome])
            .get_sample_sum();
        assert!((actual - expected).abs() < 0.001, "{actual} != {expected}");
    }

    /// Completed flows with at most this many retries
    pub fn completed_flows_retried_at_most(
        &self,
        flow_type: &str,
        outcome: &str,
        retries: u32,
    ) -> u64 {
        use prometheus::core::Metric as _;

        self.completion_metrics()
            .flow_retries
            .with_label_values(&[flow_type, outcome])
            .metric()
            .get_histogram()
            .get_bucket()
            .iter()
            .find(|bucket| (bucket.upper_bound() - f64::from(retries)).abs() < f64::EPSILON)
            .expect("retries must be a bucket bound")
            .cumulative_count()
    }

    pub fn aborted_flows(&self, flow_type: &str) -> u64 {
        self.completion_metrics()
            .flows_aborted_total
            .with_label_values(&[flow_type])
            .get()
    }

    fn completion_metrics(&self) -> Arc<FlowCompletionMetrics> {
        self.catalog.get_one::<FlowCompletionMetrics>().unwrap()
    }

    pub fn is_projector_failing(&self, projector_name: &str) -> bool {
        self.catalog
            .get_one::<FlowSystemEventAgentMetrics>()
            .unwrap()
            .projector_failing
            .with_label_values(&[projector_name])
            .get()
            > 0
    }

    /// Whether the flaky projector applied the event it failed on at first
    pub fn has_flaky_projector_applied_failed_event(&self) -> bool {
        let projector = self
            .catalog
            .get_one::<FlakyFlowSystemEventProjector>()
            .unwrap();
        let state = projector.state.lock().unwrap();
        state
            .failed_event_id
            .is_some_and(|failed_event_id| state.applied_event_ids.contains(&failed_event_id))
    }

    pub fn task_driver(&self, args: TaskDriverArgs) -> TaskDriver {
        TaskDriver::new(
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            self.catalog.get_one().unwrap(),
            args,
        )
    }

    pub fn manual_flow_trigger_driver(
        &self,
        args: ManualFlowActivationArgs,
    ) -> ManualFlowActivationDriver {
        ManualFlowActivationDriver::new(self.catalog.clone(), self.catalog.get_one().unwrap(), args)
    }

    pub fn manual_flow_abort_driver(&self, args: ManualFlowAbortArgs) -> ManualFlowAbortDriver {
        ManualFlowAbortDriver::new(self.catalog.clone(), self.catalog.get_one().unwrap(), args)
    }

    pub fn now(&self) -> DateTime<Utc> {
        self.fake_system_time_source.now()
    }

    pub async fn advance_time(&self, time_quantum: Duration) {
        self.advance_time_custom_alignment(
            Duration::milliseconds(SCHEDULING_ALIGNMENT_MS),
            time_quantum,
        )
        .await;
    }

    pub async fn advance_time_custom_alignment(&self, alignment: Duration, time_quantum: Duration) {
        // Examples:
        // 12 ÷ 4 = 3
        // 15 ÷ 4 = 4
        // 16 ÷ 4 = 4
        fn div_up(a: i64, b: i64) -> i64 {
            (a + (b - 1)) / b
        }

        let time_increments_count = div_up(
            time_quantum.num_milliseconds(),
            alignment.num_milliseconds(),
        );

        for _ in 0..time_increments_count {
            // Yield multiple times to ensure all tasks get execution time
            yield_now().await;
            yield_now().await;

            let woken_callers = self.fake_system_time_source.advance(alignment);

            // If we woke up any callers, give them extra time to process
            if !woken_callers.is_empty() {
                yield_now().await;
            }
        }

        // Final yield to ensure any remaining tasks can complete
        yield_now().await;
    }

    /// Simulates a complete flow scenario by running flow agents concurrently
    /// with a user-provided simulation script. Handles initial snapshot
    /// creation and final event catchup automatically.
    ///
    /// # Arguments
    /// * `simulation_script` - An async closure/future that contains the test
    ///   scenario logic (task drivers, manual triggers, time advancement, etc.)
    ///
    /// # Example
    /// ```rust
    /// harness.simulate_flow_scenario(async {
    ///     let task_driver = harness.task_driver(TaskDriverArgs { ... });
    ///     let task_handle = task_driver.run();
    ///
    ///     let sim_handle = harness.advance_time(Duration::milliseconds(150));
    ///     tokio::join!(task_handle, sim_handle)
    /// }).await.unwrap();
    /// ```
    pub async fn simulate_flow_scenario<F, Fut>(
        &self,
        simulation_script: F,
    ) -> Result<(), InternalError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        // Ensure flow agent is initialized
        use init_on_startup::InitOnStartup;
        self.flow_agent.run_initialization().await.unwrap();

        // Project what the initialization wrote before the initial snapshot
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await?;

        // Create initial snapshot - the state at moment 0 after flow agent loaded
        let test_flow_listener = self.catalog.get_one::<FlowSystemTestListener>().unwrap();
        test_flow_listener.mark_as_loaded();
        test_flow_listener.make_a_snapshot(self.now());

        // Run scheduler concurrently with the provided simulation script.
        // Polling order is fixed, so that the order of events written at the same
        // virtual moment by the script and the agents does not depend on chance.
        // Projections catch up on what the script wrote before the flow agent acts,
        // as they would have long done by then in a real deployment
        tokio::select! {
            biased;

            // Run the user-provided simulation script
            _ = simulation_script() => Ok(()),

            // Run flow system event agent
            _  = self.flow_system_event_agent.run() => Ok(()),

            // Run flow agent
            res = self.flow_agent.run() => res.int_err(),
        }?;

        // Catchup remaining events
        self.flow_system_event_agent
            .catchup_remaining_events()
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const FAILING_PROJECTOR_NAME: &str = "FailingFlowSystemEventProjector";

#[component(pub)]
#[interface(dyn FlowSystemEventProjector)]
pub(crate) struct FailingFlowSystemEventProjector {}

#[async_trait::async_trait]
impl FlowSystemEventProjector for FailingFlowSystemEventProjector {
    fn name(&self) -> &'static str {
        FAILING_PROJECTOR_NAME
    }

    async fn apply(&self, _: &FlowSystemEvent) -> Result<(), InternalError> {
        Err(InternalError::new("Projection failed"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) const FLAKY_PROJECTOR_NAME: &str = "FlakyFlowSystemEventProjector";

#[derive(Default)]
struct FlakyProjectorState {
    failed_event_id: Option<EventID>,
    applied_event_ids: Vec<EventID>,
}

// Singleton: the agent builds projectors per transaction, and the state must
// survive between attempts
pub(crate) struct FlakyFlowSystemEventProjector {
    state: Mutex<FlakyProjectorState>,
}

#[component(pub)]
#[interface(dyn FlowSystemEventProjector)]
#[scope(Singleton)]
impl FlakyFlowSystemEventProjector {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(FlakyProjectorState::default()),
        }
    }
}

#[async_trait::async_trait]
impl FlowSystemEventProjector for FlakyFlowSystemEventProjector {
    fn name(&self) -> &'static str {
        FLAKY_PROJECTOR_NAME
    }

    async fn apply(&self, e: &FlowSystemEvent) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();
        if state.failed_event_id.is_none() {
            state.failed_event_id = Some(e.event_id);
            return Err(InternalError::new("Projection failed once"));
        }
        state.applied_event_ids.push(e.event_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
