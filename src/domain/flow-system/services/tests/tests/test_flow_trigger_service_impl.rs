// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::{Duration, Utc};
use database_common_macros::transactional_method1;
use dill::*;
use futures::TryStreamExt;
use kamu_adapter_flow_dataset::{
    FlowDatasetsEventBridge,
    FlowScopeDataset,
    compaction_dataset_binding,
    ingest_dataset_binding,
};
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceDefault;

use super::FlowTriggerTestListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_visibility() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());

    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let binding_foo_ingest = ingest_dataset_binding(&foo_id);
    let binding_foo_compaction = compaction_dataset_binding(&foo_id);
    let binding_bar_ingest = ingest_dataset_binding(&bar_id);

    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_flow_trigger(binding_foo_ingest.clone(), foo_ingest_trigger.clone())
        .await;

    let foo_compaction_trigger = FlowTriggerRule::Schedule(Duration::days(1).into());
    harness
        .set_flow_trigger(
            binding_foo_compaction.clone(),
            foo_compaction_trigger.clone(),
        )
        .await;

    let bar_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_flow_trigger(binding_bar_ingest.clone(), bar_ingest_trigger.clone())
        .await;

    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(3, triggers.len());

    for (flow_binding, trigger) in [
        (&binding_foo_ingest, &foo_ingest_trigger),
        (&binding_foo_compaction, &foo_compaction_trigger),
        (&binding_bar_ingest, &bar_ingest_trigger),
    ] {
        harness.expect_flow_trigger(&triggers, flow_binding, trigger);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_dataset_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Make a dataset and configure daily ingestion schedule
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let binding_foo_ingest = ingest_dataset_binding(&foo_id);
    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    harness
        .set_flow_trigger(binding_foo_ingest.clone(), foo_ingest_trigger.clone())
        .await;

    // It should be visible in the list of enabled triggers and produced 1 trigger
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_flow_trigger(&triggers, &binding_foo_ingest, &foo_ingest_trigger);
    assert_eq!(1, harness.trigger_events_count());

    // Now, pause this flow trigger
    harness.pause_flow(&binding_foo_ingest).await;

    // It should disappear from the list of enabled triggers, and produce 1 more
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(2, harness.trigger_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_trigger_state = harness
        .get_flow_trigger_from_store(&binding_foo_ingest)
        .await;
    assert_eq!(flow_trigger_state.status, FlowTriggerStatus::PausedByUser);
    assert_eq!(flow_trigger_state.rule, foo_ingest_trigger.clone());

    // Now, resume the trigger
    harness.resume_flow(&binding_foo_ingest).await;

    // It should be visible in the list of active triggers again, and produce
    // another event yet
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_flow_trigger(&triggers, &binding_foo_ingest, &foo_ingest_trigger);
    assert_eq!(3, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_all_dataset_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Make a dataset and configure ingestion and compaction schedule
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");

    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    let binding_foo_ingest = ingest_dataset_binding(&foo_id);
    harness
        .set_flow_trigger(binding_foo_ingest.clone(), foo_ingest_trigger.clone())
        .await;

    let foo_compaction_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    let binding_foo_compaction = compaction_dataset_binding(&foo_id);
    harness
        .set_flow_trigger(
            binding_foo_compaction.clone(),
            foo_compaction_trigger.clone(),
        )
        .await;

    // Both should be visible in the list of enabled triggers, 2 events expected
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(2, triggers.len());
    harness.expect_flow_trigger(&triggers, &binding_foo_ingest, &foo_ingest_trigger);
    harness.expect_flow_trigger(&triggers, &binding_foo_compaction, &foo_compaction_trigger);
    assert_eq!(2, harness.trigger_events_count());

    // Now, pause all flows of this dataset
    harness.pause_all_dataset_flows(&foo_id).await;

    // Both should disappear from the list of enabled triggers,
    // and both should produce events
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(4, harness.trigger_events_count());

    // Still, we should see their state as paused in the repository directly

    let flow_trigger_ingest_state = harness
        .get_flow_trigger_from_store(&binding_foo_ingest)
        .await;
    assert_eq!(
        flow_trigger_ingest_state.status,
        FlowTriggerStatus::PausedByUser
    );
    assert_eq!(flow_trigger_ingest_state.rule, foo_ingest_trigger.clone());

    let flow_trigger_compaction_state = harness
        .get_flow_trigger_from_store(&binding_foo_compaction)
        .await;
    assert_eq!(
        flow_trigger_compaction_state.status,
        FlowTriggerStatus::PausedByUser
    );
    assert_eq!(
        flow_trigger_compaction_state.rule,
        foo_compaction_trigger.clone()
    );

    // Now, resume all triggers
    harness.resume_all_dataset_flows(&foo_id).await;

    // They should be visible in the list of active triggers again, and again,we
    // should get 2 extra events
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(2, triggers.len());
    harness.expect_flow_trigger(&triggers, &binding_foo_ingest, &foo_ingest_trigger);
    harness.expect_flow_trigger(&triggers, &binding_foo_compaction, &foo_compaction_trigger);
    assert_eq!(6, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_pause_resume_individual_system_flows() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());
    assert_eq!(0, harness.trigger_events_count());

    // Configure GC schedule
    let gc_schedule: Schedule = Duration::minutes(30).into();
    let binding_gc = FlowBinding::new(FLOW_TYPE_SYSTEM_GC, FlowScope::make_system_scope());
    harness
        .set_system_flow_schedule(FLOW_TYPE_SYSTEM_GC, gc_schedule.clone())
        .await
        .unwrap();

    // It should be visible in the list of enabled triggers, and create 1 event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_system_flow_schedule(&triggers, FLOW_TYPE_SYSTEM_GC, &gc_schedule);
    assert_eq!(1, harness.trigger_events_count());

    // Now, pause this flow trigger
    harness.pause_flow(&binding_gc).await;

    // It should disappear from the list of enabled triggers, and create 1 more
    // event
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(0, triggers.len());
    assert_eq!(2, harness.trigger_events_count());

    // Still, we should see it's state as paused in the repository directly
    let flow_trigger_state = harness.get_flow_trigger_from_store(&binding_gc).await;
    assert_eq!(flow_trigger_state.status, FlowTriggerStatus::PausedByUser);
    assert_eq!(
        flow_trigger_state.rule,
        FlowTriggerRule::Schedule(gc_schedule.clone())
    );

    // Now, resume the triggers
    harness.resume_flow(&binding_gc).await;

    // It should be visible in the list of active triggers again, and create
    // another event yet
    let triggers = harness.list_enabled_triggers().await;
    assert_eq!(1, triggers.len());
    harness.expect_system_flow_schedule(&triggers, FLOW_TYPE_SYSTEM_GC, &gc_schedule);
    assert_eq!(3, harness.trigger_events_count());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowTriggerHarness::new();
    assert!(harness.list_enabled_triggers().await.is_empty());

    // Make a dataset and configure ingest trigger
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_ingest_trigger = FlowTriggerRule::Schedule(Duration::weeks(1).into());
    let binding_foo_ingest = ingest_dataset_binding(&foo_id);
    harness
        .set_flow_trigger(binding_foo_ingest.clone(), foo_ingest_trigger.clone())
        .await;

    // It should be visible in the list of triggers
    let configs = harness.list_enabled_triggers().await;
    assert_eq!(1, configs.len());
    harness.expect_flow_trigger(&configs, &binding_foo_ingest, &foo_ingest_trigger);

    // Now, pretend the dataset was deleted
    harness.issue_dataset_deleted(&foo_id).await;

    // The dataset should not be visible in the list of configs
    let configs = harness.list_enabled_triggers().await;
    assert_eq!(0, configs.len());

    // Still, we should see it's state as permanently stopped in the repository
    let flow_config_state = harness
        .get_flow_trigger_from_store(&binding_foo_ingest)
        .await;
    assert_eq!(flow_config_state.rule, foo_ingest_trigger);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowTriggerHarness {
    catalog: Catalog,
    flow_trigger_service: Arc<dyn FlowTriggerService>,
    flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
    trigger_listener: Arc<FlowTriggerTestListener>,
    outbox: Arc<dyn Outbox>,
}

impl FlowTriggerHarness {
    fn new() -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<FlowTriggerTestListener>()
            .add::<FlowTriggerServiceImpl>()
            .add::<FlowDatasetsEventBridge>()
            .add::<FlowSensorDispatcherImpl>()
            .add::<InMemoryFlowTriggerEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryFlowProcessState>()
            .add::<SystemTimeSourceDefault>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );
            register_message_dispatcher::<FlowTriggerUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_TRIGGER_SERVICE,
            );

            b.build()
        };

        Self {
            flow_trigger_service: catalog.get_one().unwrap(),
            flow_trigger_event_store: catalog.get_one().unwrap(),
            trigger_listener: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
            catalog,
        }
    }

    async fn list_enabled_triggers(&self) -> HashMap<FlowBinding, FlowTriggerState> {
        let active_triggers: Vec<_> = self
            .flow_trigger_service
            .list_enabled_triggers()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut res = HashMap::new();
        for active_trigger in active_triggers {
            res.insert(active_trigger.flow_binding.clone(), active_trigger);
        }
        res
    }

    #[transactional_method1(flow_trigger_service: Arc<dyn FlowTriggerService>)]
    async fn set_system_flow_schedule(
        &self,
        system_flow_type: &str,
        schedule: Schedule,
    ) -> Result<(), SetFlowTriggerError> {
        flow_trigger_service
            .set_trigger(
                Utc::now(),
                FlowBinding::new(system_flow_type, FlowScope::make_system_scope()),
                FlowTriggerRule::Schedule(schedule),
                FlowTriggerStopPolicy::default(),
            )
            .await?;
        Ok(())
    }

    async fn set_flow_trigger(&self, flow_binding: FlowBinding, trigger_rule: FlowTriggerRule) {
        self.flow_trigger_service
            .set_trigger(
                Utc::now(),
                flow_binding,
                trigger_rule,
                FlowTriggerStopPolicy::default(),
            )
            .await
            .unwrap();
    }

    fn expect_flow_trigger(
        &self,
        triggers: &HashMap<FlowBinding, FlowTriggerState>,
        flow_binding: &FlowBinding,
        expected_rule: &FlowTriggerRule,
    ) {
        assert_matches!(
            triggers.get(flow_binding),
            Some(FlowTriggerState {
                status: FlowTriggerStatus::Active,
                rule: actual_rule,
                ..
            }) if actual_rule == expected_rule
        );
    }

    fn expect_system_flow_schedule(
        &self,
        enabled_triggers: &HashMap<FlowBinding, FlowTriggerState>,
        system_flow_type: &str,
        expected_schedule: &Schedule,
    ) {
        let flow_binding = FlowBinding::new(system_flow_type, FlowScope::make_system_scope());
        assert_matches!(
            enabled_triggers.get(&flow_binding),
            Some(FlowTriggerState {
                status: FlowTriggerStatus::Active,
                rule: FlowTriggerRule::Schedule(actual_schedule),
                ..
            }) if actual_schedule == expected_schedule
        );
    }

    async fn get_flow_trigger_from_store(&self, flow_binding: &FlowBinding) -> FlowTriggerState {
        let flow_trigger = FlowTrigger::load(flow_binding, self.flow_trigger_event_store.as_ref())
            .await
            .unwrap();
        flow_trigger.into()
    }

    async fn pause_flow(&self, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .pause_flow_trigger(Utc::now(), flow_binding)
            .await
            .unwrap();
    }

    async fn pause_all_dataset_flows(&self, dataset_id: &odf::DatasetID) {
        let lookup_scopes = vec![FlowScopeDataset::make_scope(dataset_id)];
        self.flow_trigger_service
            .pause_flow_triggers_for_scopes(Utc::now(), &lookup_scopes)
            .await
            .unwrap();
    }

    async fn resume_flow(&self, flow_binding: &FlowBinding) {
        self.flow_trigger_service
            .resume_flow_trigger(Utc::now(), flow_binding)
            .await
            .unwrap();
    }

    async fn resume_all_dataset_flows(&self, dataset_id: &odf::DatasetID) {
        let lookup_scopes = vec![FlowScopeDataset::make_scope(dataset_id)];
        self.flow_trigger_service
            .resume_flow_triggers_for_scopes(Utc::now(), &lookup_scopes)
            .await
            .unwrap();
    }

    fn trigger_events_count(&self) -> usize {
        self.trigger_listener.triggers_events_count()
    }

    async fn issue_dataset_deleted(&self, dataset_id: &odf::DatasetID) {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::deleted(dataset_id.clone()),
            )
            .await
            .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
