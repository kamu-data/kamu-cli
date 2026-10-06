// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu_flow_system::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Projects flow events into links between upstream flows and the downstream
/// flows they activated
#[dill::component(pub)]
#[dill::interface(dyn FlowSystemEventProjector)]
pub struct FlowActivationLinkProjector {
    flow_activation_link_repository: Arc<dyn FlowActivationLinkRepository>,
    flow_event_store: Arc<dyn FlowEventStore>,
    upstream_extractors: Vec<Arc<dyn FlowActivationCauseUpstreamExtractor>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Only these flow events carry activation causes
const CAUSE_CARRYING_FLOW_EVENT_TAGS: [&str; 2] = ["Initiated", "ActivationCauseAdded"];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowActivationLinkProjector {
    fn extract_upstream_flow_id(&self, activation_cause: &FlowActivationCause) -> Option<FlowID> {
        match activation_cause {
            FlowActivationCause::ResourceUpdate(update) => {
                let Some(extractor) = self
                    .upstream_extractors
                    .iter()
                    .find(|extractor| extractor.resource_type() == update.resource_type)
                else {
                    tracing::debug!(
                        resource_type = %update.resource_type,
                        "No upstream extractor for resource type"
                    );
                    return None;
                };
                extractor.extract_upstream_flow_id(update)
            }
            FlowActivationCause::Manual(_)
            | FlowActivationCause::AutoPolling(_)
            | FlowActivationCause::IterationFinished(_) => None,
        }
    }

    async fn save_link(
        &self,
        upstream_flow_id: FlowID,
        downstream_flow_id: FlowID,
        activated_at: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        tracing::debug!(
            %upstream_flow_id,
            %downstream_flow_id,
            "Saving flow activation link"
        );

        self.flow_activation_link_repository
            .save_link(&FlowActivationLink {
                upstream_flow_id,
                downstream_flow_id,
                activated_at,
            })
            .await
    }

    /// A cause added after the flow got a task is late: the flow does not
    /// process it, it is handed to another flow or dropped on completion. A
    /// cause never moves between the two lists, so the latest state answers
    /// this for any past event.
    async fn is_cause_processed_by_flow(
        &self,
        flow_id: FlowID,
        upstream_flow_id: FlowID,
    ) -> Result<bool, InternalError> {
        match Flow::try_load(flow_id, self.flow_event_store.as_ref()).await {
            Ok(Some(flow)) => Ok(flow
                .activation_causes
                .iter()
                .any(|cause| self.extract_upstream_flow_id(cause) == Some(upstream_flow_id))),
            Ok(None) => {
                tracing::warn!(%flow_id, "Flow with an added activation cause does not exist");
                Ok(false)
            }
            Err(TryLoadError::ProjectionError(e)) => {
                tracing::warn!(%flow_id, error = ?e, "Flow with an added activation cause cannot be loaded");
                Ok(false)
            }
            Err(TryLoadError::Internal(e)) => Err(e),
        }
    }

    async fn process_flow_event(&self, flow_event: FlowEvent) -> Result<(), InternalError> {
        match flow_event {
            FlowEvent::Initiated(e) => {
                if let Some(upstream_flow_id) = self.extract_upstream_flow_id(&e.activation_cause) {
                    self.save_link(upstream_flow_id, e.flow_id, e.event_time)
                        .await?;
                }
            }

            FlowEvent::ActivationCauseAdded(e) => {
                if let Some(upstream_flow_id) = self.extract_upstream_flow_id(&e.activation_cause)
                    && self
                        .is_cause_processed_by_flow(e.flow_id, upstream_flow_id)
                        .await?
                {
                    self.save_link(upstream_flow_id, e.flow_id, e.event_time)
                        .await?;
                }
            }

            FlowEvent::StartConditionUpdated(_)
            | FlowEvent::ConfigSnapshotModified(_)
            | FlowEvent::ScheduledForActivation(_)
            | FlowEvent::TaskScheduled(_)
            | FlowEvent::TaskRunning(_)
            | FlowEvent::TaskFinished(_)
            | FlowEvent::Completed(_)
            | FlowEvent::Aborted(_) => { /* No activation causes */ }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowSystemEventProjector for FlowActivationLinkProjector {
    fn name(&self) -> &'static str {
        FLOW_ACTIVATION_LINK_PROJECTOR_NAME
    }

    #[tracing::instrument(level = "debug", skip_all, fields(event_id=%event.event_id))]
    async fn apply(&self, event: &FlowSystemEvent) -> Result<(), InternalError> {
        match event.source_type {
            FlowSystemEventSourceType::FlowConfiguration
            | FlowSystemEventSourceType::FlowTrigger => Ok(()),

            FlowSystemEventSourceType::Flow => {
                // Flow events are externally tagged: skip the ones without causes unparsed
                let carries_causes = event.payload.as_object().is_some_and(|payload| {
                    payload
                        .keys()
                        .any(|tag| CAUSE_CARRYING_FLOW_EVENT_TAGS.contains(&tag.as_str()))
                });
                if !carries_causes {
                    return Ok(());
                }

                let flow_event: FlowEvent =
                    serde_json::from_value(event.payload.clone()).int_err()?;
                self.process_flow_event(flow_event).await
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
