// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;
use std::sync::Arc;

use chrono::{DateTime, Duration, SubsecRound, Utc};
use dill::*;
use futures::TryStreamExt;
use kamu_adapter_flow_dataset::*;
use kamu_flow_system::*;
use kamu_flow_system_inmem::*;
use kamu_flow_system_services::*;
use kamu_task_system::TaskID;
use kamu_wakeup_listener_inmem::InMemoryWakeupHub;
use pretty_assertions::assert_eq;
use wakeup_listener::{WakeupListenerConfig, WakeupListenerMetrics};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_initial_cause_links_upstream_flow() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id = harness.new_flow_id().await;
    let downstream_flow_id = harness
        .initiate_flow(harness.upstream_flow_cause(upstream_flow_id))
        .await;

    harness.catchup().await;

    assert_eq!(
        harness.get_downstream_links(&[upstream_flow_id]).await,
        vec![harness.link(upstream_flow_id, downstream_flow_id)],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_cause_added_before_task_links_upstream_flow() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id_1 = harness.new_flow_id().await;
    let upstream_flow_id_2 = harness.new_flow_id().await;

    let mut downstream_flow = harness
        .initiate_flow_aggregate(harness.upstream_flow_cause(upstream_flow_id_1))
        .await;
    harness
        .add_cause(
            &mut downstream_flow,
            harness.upstream_flow_cause(upstream_flow_id_2),
        )
        .await;
    harness.schedule_task(&mut downstream_flow).await;

    harness.catchup().await;

    assert_eq!(
        harness
            .get_downstream_links(&[upstream_flow_id_1, upstream_flow_id_2])
            .await,
        vec![
            harness.link(upstream_flow_id_1, downstream_flow.flow_id),
            harness.link(upstream_flow_id_2, downstream_flow.flow_id),
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_one_upstream_flow_activates_several_downstream_flows() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id = harness.new_flow_id().await;
    let downstream_flow_id_1 = harness
        .initiate_flow(harness.upstream_flow_cause(upstream_flow_id))
        .await;
    let downstream_flow_id_2 = harness
        .initiate_flow(harness.upstream_flow_cause(upstream_flow_id))
        .await;

    harness.catchup().await;

    assert_eq!(
        harness.get_downstream_links(&[upstream_flow_id]).await,
        vec![
            harness.link(upstream_flow_id, downstream_flow_id_1),
            harness.link(upstream_flow_id, downstream_flow_id_2),
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_late_cause_links_only_the_flow_it_moves_to() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id_1 = harness.new_flow_id().await;
    let upstream_flow_id_2 = harness.new_flow_id().await;

    let mut running_flow = harness
        .initiate_flow_aggregate(harness.upstream_flow_cause(upstream_flow_id_1))
        .await;
    harness.schedule_task(&mut running_flow).await;
    harness
        .add_cause(
            &mut running_flow,
            harness.upstream_flow_cause(upstream_flow_id_2),
        )
        .await;

    harness.catchup().await;
    assert_eq!(
        harness.get_downstream_links(&[upstream_flow_id_2]).await,
        vec![],
    );

    let next_flow_id = harness
        .initiate_flow(harness.upstream_flow_cause(upstream_flow_id_2))
        .await;

    harness.catchup().await;
    assert_eq!(
        harness
            .get_downstream_links(&[upstream_flow_id_1, upstream_flow_id_2])
            .await,
        vec![
            harness.link(upstream_flow_id_1, running_flow.flow_id),
            harness.link(upstream_flow_id_2, next_flow_id),
        ],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_late_cause_of_aborted_flow_is_not_linked() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id = harness.new_flow_id().await;

    let mut flow = harness
        .initiate_flow_aggregate(harness.manual_cause())
        .await;
    harness.schedule_task(&mut flow).await;
    harness
        .add_cause(&mut flow, harness.upstream_flow_cause(upstream_flow_id))
        .await;
    harness.abort(&mut flow).await;

    harness.catchup().await;

    assert_eq!(
        harness.get_downstream_links(&[upstream_flow_id]).await,
        vec![],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_causes_without_upstream_flow_are_ignored() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let unrelated_flow_id = harness.new_flow_id().await;

    let mut flow = harness
        .initiate_flow_aggregate(harness.manual_cause())
        .await;
    for cause in [
        harness.auto_polling_cause(),
        harness.dataset_update_cause(DatasetUpdateSource::HttpIngest { source_name: None }),
        harness.dataset_update_cause(DatasetUpdateSource::ExternallyDetectedChange),
        harness.resource_update_cause(
            "dev.kamu.resource.UnknownResource",
            serde_json::json!({ "flow_id": unrelated_flow_id }),
        ),
        harness.resource_update_cause(
            DATASET_RESOURCE_TYPE,
            serde_json::json!({ "unexpected": "shape" }),
        ),
    ] {
        harness.add_cause(&mut flow, cause).await;
    }
    harness.schedule_task(&mut flow).await;

    harness.catchup().await;

    assert!(!harness.is_projector_failing());
    assert_eq!(
        harness.get_downstream_links(&[unrelated_flow_id]).await,
        vec![],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_reapplying_events_is_idempotent() {
    let harness = FlowActivationLinkProjectorHarness::new();

    let upstream_flow_id_1 = harness.new_flow_id().await;
    let upstream_flow_id_2 = harness.new_flow_id().await;

    let mut downstream_flow = harness
        .initiate_flow_aggregate(harness.upstream_flow_cause(upstream_flow_id_1))
        .await;
    harness
        .add_cause(
            &mut downstream_flow,
            harness.upstream_flow_cause(upstream_flow_id_2),
        )
        .await;
    harness.schedule_task(&mut downstream_flow).await;

    harness.catchup().await;
    let links = harness
        .get_downstream_links(&[upstream_flow_id_1, upstream_flow_id_2])
        .await;

    harness.reapply_flow_events(downstream_flow.flow_id).await;

    assert_eq!(
        harness
            .get_downstream_links(&[upstream_flow_id_1, upstream_flow_id_2])
            .await,
        links,
    );
    assert_eq!(links.len(), 2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FlowActivationLinkProjectorHarness {
    catalog: Catalog,
    flow_event_store: Arc<dyn FlowEventStore>,
    flow_activation_link_repository: Arc<dyn FlowActivationLinkRepository>,
    projector: Arc<FlowActivationLinkProjector>,
    now: DateTime<Utc>,
}

impl FlowActivationLinkProjectorHarness {
    fn new() -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            b.add_value(FlowSystemEventAgentConfig {
                batch_size: NonZeroUsize::new(10).unwrap(),
            })
            .add_value(WakeupListenerConfig::local_default())
            .add::<FlowSystemEventAgentImpl>()
            .add::<FlowSystemEventAgentMetrics>()
            .add::<FlowActivationLinkProjector>()
            .add::<DatasetResourceUpstreamFlowExtractor>()
            .add::<InMemoryFlowActivationLinkRepository>()
            .add::<InMemoryFlowEventStore>()
            .add::<InMemoryFlowSystemEventBridge>()
            .add::<InMemoryWakeupHub>()
            .add::<WakeupListenerMetrics>();

            database_common::NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        Self {
            flow_event_store: catalog.get_one().unwrap(),
            flow_activation_link_repository: catalog.get_one().unwrap(),
            projector: catalog.get_one().unwrap(),
            now: Utc::now().trunc_subsecs(0),
            catalog,
        }
    }

    async fn new_flow_id(&self) -> FlowID {
        self.flow_event_store.new_flow_id().await.unwrap()
    }

    fn link(&self, upstream_flow_id: FlowID, downstream_flow_id: FlowID) -> FlowActivationLink {
        FlowActivationLink {
            upstream_flow_id,
            downstream_flow_id,
            activated_at: self.now,
        }
    }

    fn manual_cause(&self) -> FlowActivationCause {
        FlowActivationCause::Manual(FlowActivationCauseManual {
            activation_time: self.now,
            initiator_account_id: odf::AccountID::new_seeded_ed25519(b"alice"),
        })
    }

    fn auto_polling_cause(&self) -> FlowActivationCause {
        FlowActivationCause::AutoPolling(FlowActivationCauseAutoPolling {
            activation_time: self.now,
        })
    }

    fn upstream_flow_cause(&self, upstream_flow_id: FlowID) -> FlowActivationCause {
        self.dataset_update_cause(DatasetUpdateSource::UpstreamFlow {
            flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
            flow_id: upstream_flow_id,
            maybe_flow_config_snapshot: None,
        })
    }

    fn dataset_update_cause(&self, source: DatasetUpdateSource) -> FlowActivationCause {
        self.resource_update_cause(
            DATASET_RESOURCE_TYPE,
            serde_json::to_value(DatasetResourceUpdateDetails {
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                source,
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"head"),
            })
            .unwrap(),
        )
    }

    fn resource_update_cause(
        &self,
        resource_type: &str,
        details: serde_json::Value,
    ) -> FlowActivationCause {
        FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
            activation_time: self.now,
            changes: ResourceChanges::NewData(ResourceDataChanges::default()),
            resource_type: resource_type.to_string(),
            details,
        })
    }

    async fn initiate_flow(&self, activation_cause: FlowActivationCause) -> FlowID {
        self.initiate_flow_aggregate(activation_cause).await.flow_id
    }

    async fn initiate_flow_aggregate(&self, activation_cause: FlowActivationCause) -> Flow {
        let flow_id = self.new_flow_id().await;
        let mut flow = Flow::new(
            self.now,
            flow_id,
            transform_dataset_binding(&odf::DatasetID::new_seeded_ed25519(b"bar")),
            activation_cause,
            None,
            None,
        );
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
        flow
    }

    async fn add_cause(&self, flow: &mut Flow, activation_cause: FlowActivationCause) {
        assert!(
            flow.add_activation_cause_if_unique(self.now, activation_cause)
                .unwrap()
        );
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }

    async fn schedule_task(&self, flow: &mut Flow) {
        flow.schedule_for_activation(self.now, self.now).unwrap();
        flow.on_task_scheduled(self.now, TaskID::new(1)).unwrap();
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }

    async fn abort(&self, flow: &mut Flow) {
        flow.abort(self.now + Duration::seconds(1)).unwrap();
        flow.save(self.flow_event_store.as_ref()).await.unwrap();
    }

    async fn catchup(&self) {
        self.catalog
            .get_one::<dyn FlowSystemEventAgent>()
            .unwrap()
            .catchup_remaining_events()
            .await
            .unwrap();
    }

    fn is_projector_failing(&self) -> bool {
        self.catalog
            .get_one::<FlowSystemEventAgentMetrics>()
            .unwrap()
            .projector_failing
            .with_label_values(&[FLOW_ACTIVATION_LINK_PROJECTOR_NAME])
            .get()
            > 0
    }

    /// Applies the flow's events to the projector once more, as a retried
    /// batch would
    async fn reapply_flow_events(&self, flow_id: FlowID) {
        let flow_events: Vec<_> = self
            .flow_event_store
            .get_events(&flow_id, Default::default())
            .try_collect()
            .await
            .unwrap();

        for (event_id, flow_event) in flow_events {
            self.projector
                .apply(&FlowSystemEvent {
                    event_id,
                    tx_id: 0,
                    source_type: FlowSystemEventSourceType::Flow,
                    occurred_at: flow_event.event_time(),
                    payload: serde_json::to_value(flow_event).unwrap(),
                })
                .await
                .unwrap();
        }
    }

    async fn get_downstream_links(&self, upstream_flow_ids: &[FlowID]) -> Vec<FlowActivationLink> {
        self.flow_activation_link_repository
            .get_downstream_links(upstream_flow_ids)
            .await
            .unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
