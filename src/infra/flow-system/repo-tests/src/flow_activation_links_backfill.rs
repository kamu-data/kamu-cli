// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Duration, DurationRound, Utc};
use dill::Catalog;
use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_INGEST,
    transform_dataset_binding,
};
use kamu_flow_system::*;
use kamu_task_system::TaskID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Flow history written through the real event store, covering every rule of
/// the activation links backfill migration
pub struct FlowActivationLinksBackfillScenario {
    pub upstream_flow_ids: Vec<FlowID>,
    pub expected_links: Vec<FlowActivationLink>,
}

impl FlowActivationLinksBackfillScenario {
    pub async fn write(catalog: &Catalog) -> Self {
        let writer = HistoryWriter::new(catalog);

        let initial_upstream_flow_id = writer.new_flow_id().await;
        let added_upstream_flow_id = writer.new_flow_id().await;
        let late_upstream_flow_id = writer.new_flow_id().await;

        // Initial cause, then a cause added before the task: both linked
        let mut batched_flow = writer
            .initiate_flow(writer.upstream_flow_cause(initial_upstream_flow_id))
            .await;
        writer
            .add_cause(
                &mut batched_flow,
                writer.upstream_flow_cause(added_upstream_flow_id),
            )
            .await;
        writer.schedule_task(&mut batched_flow).await;

        // A cause added after the task is late: not linked to this flow
        writer
            .add_cause(
                &mut batched_flow,
                writer.upstream_flow_cause(late_upstream_flow_id),
            )
            .await;

        // The late cause, moved to the next flow: linked there
        let next_flow = writer
            .initiate_flow(writer.upstream_flow_cause(late_upstream_flow_id))
            .await;

        // Causes not produced by flows, or of another resource type: never linked
        let mut unrelated_flow = writer.initiate_flow(writer.manual_cause()).await;
        for cause in [
            writer.dataset_update_cause(DatasetUpdateSource::HttpIngest { source_name: None }),
            writer.dataset_update_cause(DatasetUpdateSource::ExternallyDetectedChange),
            writer.other_resource_update_cause(initial_upstream_flow_id),
        ] {
            writer.add_cause(&mut unrelated_flow, cause).await;
        }

        Self {
            upstream_flow_ids: vec![
                initial_upstream_flow_id,
                added_upstream_flow_id,
                late_upstream_flow_id,
            ],
            expected_links: vec![
                writer.link(initial_upstream_flow_id, batched_flow.flow_id),
                writer.link(added_upstream_flow_id, batched_flow.flow_id),
                writer.link(late_upstream_flow_id, next_flow.flow_id),
            ],
        }
    }

    /// Writes one flow event after the history, which the projector must pick
    /// up
    pub async fn write_event_after_history(catalog: &Catalog) {
        let writer = HistoryWriter::new(catalog);
        writer.initiate_flow(writer.manual_cause()).await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Holds the event store of one transaction: never kept past it
struct HistoryWriter {
    flow_event_store: Arc<dyn FlowEventStore>,
    now: DateTime<Utc>,
}

impl HistoryWriter {
    fn new(catalog: &Catalog) -> Self {
        Self {
            flow_event_store: catalog.get_one().unwrap(),
            // Storage keeps sub-second precision differently, whole seconds compare
            // reliably
            now: Utc::now().duration_round(Duration::seconds(1)).unwrap(),
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

    fn upstream_flow_cause(&self, upstream_flow_id: FlowID) -> FlowActivationCause {
        self.dataset_update_cause(DatasetUpdateSource::UpstreamFlow {
            flow_type: FLOW_TYPE_DATASET_INGEST.to_string(),
            flow_id: upstream_flow_id,
            maybe_flow_config_snapshot: None,
        })
    }

    fn dataset_update_cause(&self, source: DatasetUpdateSource) -> FlowActivationCause {
        FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
            activation_time: self.now,
            changes: ResourceChanges::NewData(ResourceDataChanges::default()),
            resource_type: DATASET_RESOURCE_TYPE.to_string(),
            details: serde_json::to_value(DatasetResourceUpdateDetails {
                dataset_id: odf::DatasetID::new_seeded_ed25519(b"foo"),
                source,
                old_head_maybe: None,
                new_head: odf::Multihash::from_digest_sha3_256(b"head"),
            })
            .unwrap(),
        })
    }

    /// Details shaped like a dataset update from a flow, under a resource type
    /// the dataset extractor does not handle
    fn other_resource_update_cause(&self, upstream_flow_id: FlowID) -> FlowActivationCause {
        let FlowActivationCause::ResourceUpdate(update) =
            self.upstream_flow_cause(upstream_flow_id)
        else {
            unreachable!()
        };
        FlowActivationCause::ResourceUpdate(FlowActivationCauseResourceUpdate {
            resource_type: "dev.kamu.resource.OtherResource".to_string(),
            ..update
        })
    }

    async fn initiate_flow(&self, activation_cause: FlowActivationCause) -> Flow {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
