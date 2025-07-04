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

use dill::*;
use futures::TryStreamExt;
use kamu_adapter_flow_dataset as afs;
use kamu_adapter_flow_dataset::{
    FlowConfigRuleCompact,
    FlowConfigRuleCompactFull,
    FlowConfigRuleIngest,
};
use kamu_datasets::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_DATASET_SERVICE};
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use messaging_outbox::{Outbox, OutboxExt, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceDefault;

use super::FlowConfigTestListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_visibility() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());

    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let bar_id = odf::DatasetID::new_seeded_ed25519(b"bar");

    let foo_ingest_binding =
        FlowBinding::for_dataset(foo_id.clone(), afs::FLOW_TYPE_DATASET_INGEST);
    let foo_ingest_config = FlowConfigRuleIngest {
        fetch_uncacheable: false,
    }
    .into_flow_config();

    harness
        .set_dataset_flow_config(foo_ingest_binding.clone(), foo_ingest_config.clone(), None)
        .await;

    let foo_compaction_binding =
        FlowBinding::for_dataset(foo_id.clone(), afs::FLOW_TYPE_DATASET_COMPACT);
    let foo_compaction_config =
        FlowConfigRuleCompact::Full(FlowConfigRuleCompactFull::new_checked(2, 3, false).unwrap())
            .into_flow_config();

    harness
        .set_dataset_flow_config(
            foo_compaction_binding.clone(),
            foo_compaction_config.clone(),
            None, // No retry policy
        )
        .await;

    let bar_compaction_binding =
        FlowBinding::for_dataset(bar_id.clone(), afs::FLOW_TYPE_DATASET_COMPACT);
    let bar_compaction_config =
        FlowConfigRuleCompact::Full(FlowConfigRuleCompactFull::new_checked(3, 4, false).unwrap())
            .into_flow_config();

    harness
        .set_dataset_flow_config(
            bar_compaction_binding.clone(),
            bar_compaction_config.clone(),
            None, // No retry policy
        )
        .await;

    let configs = harness.list_active_configurations().await;
    assert_eq!(3, configs.len());

    for (flow_binding, config) in [
        (&foo_ingest_binding, &foo_ingest_config),
        (&foo_compaction_binding, &foo_compaction_config),
        (&bar_compaction_binding, &bar_compaction_config),
    ] {
        harness.expect_dataset_flow_config(&configs, flow_binding, config, None);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_modify() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    // Make a dataset and configure compaction config
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_compaction_binding =
        FlowBinding::for_dataset(foo_id.clone(), afs::FLOW_TYPE_DATASET_COMPACT);
    let foo_compaction_config =
        FlowConfigRuleCompact::Full(FlowConfigRuleCompactFull::new_checked(1, 2, false).unwrap())
            .into_flow_config();

    harness
        .set_dataset_flow_config(
            foo_compaction_binding.clone(),
            foo_compaction_config.clone(),
            None, // No retry policy
        )
        .await;

    // It should be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        &foo_compaction_binding,
        &foo_compaction_config,
        None,
    );

    // Now make the config with different parameters
    let foo_compaction_config_2 =
        FlowConfigRuleCompact::Full(FlowConfigRuleCompactFull::new_checked(2, 3, false).unwrap())
            .into_flow_config();

    harness
        .set_dataset_flow_config(
            foo_compaction_binding.clone(),
            foo_compaction_config_2.clone(),
            None, // No retry policy
        )
        .await;

    // Observe the updated config
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        &foo_compaction_binding,
        &foo_compaction_config_2,
        None,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_config_with_retry() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());
    assert_eq!(0, harness.configuration_events_count());

    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_ingest_binding =
        FlowBinding::for_dataset(foo_id.clone(), afs::FLOW_TYPE_DATASET_INGEST);
    let foo_ingest_config = FlowConfigRuleIngest {
        fetch_uncacheable: false,
    }
    .into_flow_config();

    let retry_policy = RetryPolicy {
        max_attempts: 3,
        min_delay_seconds: 30,
        backoff_type: RetryBackoffType::Exponential,
    };

    harness
        .set_dataset_flow_config(
            foo_ingest_binding.clone(),
            foo_ingest_config.clone(),
            Some(retry_policy),
        )
        .await;

    // It should be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(
        &configs,
        &foo_ingest_binding,
        &foo_ingest_config,
        Some(retry_policy),
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_deleted() {
    let harness = FlowConfigurationHarness::new();
    assert!(harness.list_active_configurations().await.is_empty());

    // Make a dataset and configure ingest rule
    let foo_id = odf::DatasetID::new_seeded_ed25519(b"foo");
    let foo_ingest_binding =
        FlowBinding::for_dataset(foo_id.clone(), afs::FLOW_TYPE_DATASET_INGEST);
    let foo_ingest_config = FlowConfigRuleIngest {
        fetch_uncacheable: true,
    }
    .into_flow_config();

    harness
        .set_dataset_flow_config(foo_ingest_binding.clone(), foo_ingest_config.clone(), None)
        .await;

    // It should be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(1, configs.len());
    harness.expect_dataset_flow_config(&configs, &foo_ingest_binding, &foo_ingest_config, None);

    // Now, pretend dataset was deleted
    harness.issue_dataset_deleted(&foo_id).await;

    // The dataset should not be visible in the list of configs
    let configs = harness.list_active_configurations().await;
    assert_eq!(0, configs.len());

    // Still, we should see it's state as permanently stopped in the repository
    let flow_config_state = harness
        .get_dataset_flow_config_from_store(&foo_ingest_binding)
        .await;
    assert_eq!(
        flow_config_state.rule,
        FlowConfigRuleIngest {
            fetch_uncacheable: true
        }
        .into_flow_config()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowConfigurationHarness {
    flow_configuration_service: Arc<dyn FlowConfigurationService>,
    flow_configuration_event_store: Arc<dyn FlowConfigurationEventStore>,
    config_listener: Arc<FlowConfigTestListener>,
    outbox: Arc<dyn Outbox>,
}

impl FlowConfigurationHarness {
    fn new() -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<FlowConfigTestListener>()
            .add::<FlowConfigurationServiceImpl>()
            .add::<InMemoryFlowConfigurationEventStore>()
            .add::<SystemTimeSourceDefault>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            register_message_dispatcher::<DatasetLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
            );
            register_message_dispatcher::<FlowConfigurationUpdatedMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_FLOW_CONFIGURATION_SERVICE,
            );

            b.build()
        };

        Self {
            flow_configuration_service: catalog.get_one().unwrap(),
            flow_configuration_event_store: catalog.get_one().unwrap(),
            config_listener: catalog.get_one().unwrap(),
            outbox: catalog.get_one().unwrap(),
        }
    }

    async fn list_active_configurations(&self) -> HashMap<FlowBinding, FlowConfigurationState> {
        let active_configs: Vec<_> = self
            .flow_configuration_service
            .list_active_configurations()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let mut res = HashMap::new();
        for active_config in active_configs {
            res.insert(active_config.flow_binding.clone(), active_config);
        }
        res
    }

    async fn set_dataset_flow_config(
        &self,
        flow_binding: FlowBinding,
        configuration_rule: FlowConfigurationRule,
        maybe_retry_policy: Option<RetryPolicy>,
    ) {
        self.flow_configuration_service
            .set_configuration(flow_binding, configuration_rule, maybe_retry_policy)
            .await
            .unwrap();
    }

    fn expect_dataset_flow_config(
        &self,
        configurations: &HashMap<FlowBinding, FlowConfigurationState>,
        flow_binding: &FlowBinding,
        expected_rule: &FlowConfigurationRule,
        expected_retry_policy: Option<RetryPolicy>,
    ) {
        assert_matches!(
            configurations.get(flow_binding),
            Some(FlowConfigurationState {
                status: FlowConfigurationStatus::Active,
                rule: actual_rule,
                retry_policy: actual_retry_policy,
                ..
            }) if actual_rule == expected_rule && *actual_retry_policy == expected_retry_policy
        );
    }

    async fn get_dataset_flow_config_from_store(
        &self,
        flow_binding: &FlowBinding,
    ) -> FlowConfigurationState {
        let flow_configuration =
            FlowConfiguration::load(flow_binding, self.flow_configuration_event_store.as_ref())
                .await
                .unwrap();
        flow_configuration.into()
    }

    fn configuration_events_count(&self) -> usize {
        self.config_listener.configuration_events_count()
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
