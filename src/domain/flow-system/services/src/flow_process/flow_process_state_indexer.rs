// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common_macros::{transactional_method1, transactional_method3};
use event_sourcing::{EventID, GetEventsOpts};
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_flow_system::{
    FlowEvent,
    FlowEventStore,
    FlowProcessStateQuery,
    FlowTriggerEvent,
    FlowTriggerEventStore,
    FlowTriggerStatus,
    FlowTriggerStopPolicy,
    JOB_KAMU_FLOW_AGENT_RECOVERY,
    JOB_KAMU_FLOW_PROCESS_STATE_INDEXER,
};
use kamu_task_system::TaskOutcome;

use crate::FlowProcessStateProjector;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: JOB_KAMU_FLOW_PROCESS_STATE_INDEXER,
    depends_on: &[
        JOB_KAMU_FLOW_AGENT_RECOVERY,
    ],
    requires_transaction: false,
})]
pub struct FlowProcessStateIndexer {
    catalog: dill::Catalog,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowProcessStateIndexer {
    const BATCH_SIZE: i64 = 1000;

    #[transactional_method1(flow_process_state_query: Arc<dyn FlowProcessStateQuery>)]
    async fn has_any_process_states(&self) -> Result<bool, InternalError> {
        flow_process_state_query.has_any_process_states().await
    }

    #[transactional_method3(
        flow_trigger_event_store: Arc<dyn FlowTriggerEventStore>,
        flow_event_store: Arc<dyn FlowEventStore>,
        flow_process_state_projector: Arc<FlowProcessStateProjector>
    )]
    async fn recover_flow_process_states(&self) -> Result<(), InternalError> {
        // Trigger events go first
        self.replay_flow_trigger_events(
            flow_trigger_event_store.as_ref(),
            flow_process_state_projector.as_ref(),
        )
        .await?;

        // Flow events follow
        self.replay_flow_events(
            flow_event_store.as_ref(),
            flow_process_state_projector.as_ref(),
        )
        .await?;

        // Success
        Ok(())
    }

    async fn replay_flow_trigger_events(
        &self,
        flow_trigger_event_store: &dyn FlowTriggerEventStore,
        flow_process_state_projector: &FlowProcessStateProjector,
    ) -> Result<(), InternalError> {
        let mut event_offset = 0;

        loop {
            use futures::TryStreamExt;

            let batch: Vec<_> = flow_trigger_event_store
                .get_all_events(GetEventsOpts {
                    from: Some(EventID::new(event_offset)),
                    to: Some(EventID::new(event_offset + Self::BATCH_SIZE)),
                })
                .try_collect()
                .await
                .int_err()?;

            let actual_batch_size = batch.len();

            for (event_id, event) in batch {
                match event {
                    FlowTriggerEvent::Created(e) => {
                        flow_process_state_projector
                            .handle_trigger_updated(
                                event_id,
                                &e.flow_binding,
                                if e.paused {
                                    FlowTriggerStatus::PausedByUser
                                } else {
                                    FlowTriggerStatus::Active
                                },
                                e.stop_policy,
                            )
                            .await?;
                    }
                    FlowTriggerEvent::Modified(e) => {
                        flow_process_state_projector
                            .handle_trigger_updated(
                                event_id,
                                &e.flow_binding,
                                if e.paused {
                                    FlowTriggerStatus::PausedByUser
                                } else {
                                    FlowTriggerStatus::Active
                                },
                                e.stop_policy,
                            )
                            .await?;
                    }
                    FlowTriggerEvent::AutoStopped(e) => {
                        flow_process_state_projector
                            .handle_trigger_updated(
                                event_id,
                                &e.flow_binding,
                                FlowTriggerStatus::StoppedAutomatically,
                                FlowTriggerStopPolicy::default(), // TODO: wrong, we don't know it
                            )
                            .await?;
                    }
                    FlowTriggerEvent::ScopeRemoved(_) => {
                        // TODO: direct delete method
                        // Ignored for now
                    }
                }
            }

            if actual_batch_size < usize::try_from(Self::BATCH_SIZE).unwrap() {
                break;
            }
            event_offset += Self::BATCH_SIZE;
        }

        Ok(())
    }

    async fn replay_flow_events(
        &self,
        flow_event_store: &dyn FlowEventStore,
        flow_process_state_projector: &FlowProcessStateProjector,
    ) -> Result<(), InternalError> {
        let mut event_offset = 0;
        let mut binding_by_flow_id = HashMap::new();

        loop {
            use futures::TryStreamExt;

            let batch: Vec<_> = flow_event_store
                .get_all_events(GetEventsOpts {
                    from: Some(EventID::new(event_offset)),
                    to: Some(EventID::new(event_offset + Self::BATCH_SIZE)),
                })
                .try_collect()
                .await
                .int_err()?;

            let actual_batch_size = batch.len();

            for (event_id, event) in batch {
                match event {
                    FlowEvent::Initiated(e) => {
                        binding_by_flow_id.insert(e.flow_id, e.flow_binding);
                    }

                    FlowEvent::ScheduledForActivation(e) => {
                        let flow_binding = binding_by_flow_id
                            .get(&e.flow_id)
                            .expect("must be filled from Initiated event");

                        flow_process_state_projector
                            .handle_flow_scheduled(
                                event_id,
                                flow_binding,
                                e.scheduled_for_activation_at,
                            )
                            .await?;
                    }

                    FlowEvent::TaskFinished(e) => {
                        let flow_binding = binding_by_flow_id
                            .get(&e.flow_id)
                            .expect("must be filled from Initiated event");

                        let last_task_in_flow = match e.task_outcome {
                            TaskOutcome::Success(_) | TaskOutcome::Cancelled => true,
                            TaskOutcome::Failed(_) => e.next_attempt_at.is_none(),
                        };

                        if last_task_in_flow {
                            flow_process_state_projector
                                .handle_flow_finished(
                                    event_id,
                                    flow_binding,
                                    &e.task_outcome.into(),
                                    e.event_time,
                                )
                                .await?;
                        }
                    }

                    FlowEvent::StartConditionUpdated(_)
                    | FlowEvent::ConfigSnapshotModified(_)
                    | FlowEvent::ActivationCauseAdded(_)
                    | FlowEvent::TaskScheduled(_)
                    | FlowEvent::TaskRunning(_)
                    | FlowEvent::Aborted(_) => {
                        // Ignored
                    }
                }
            }

            if actual_batch_size < usize::try_from(Self::BATCH_SIZE).unwrap() {
                break;
            }
            event_offset += Self::BATCH_SIZE;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for FlowProcessStateIndexer {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "FlowProcessStateIndexer::run_initialization"
    )]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // TODO: consider partial projections - where we would store the last processed
        // event ID in DB and only process events after that ID

        // If there are no process states, we should initiate recovery
        if !self.has_any_process_states().await? {
            tracing::info!("No flow process states found, initiating recovery");
            match self.recover_flow_process_states().await {
                Ok(_) => {
                    tracing::info!("Flow process state recovery completed successfully");
                }
                Err(e) => {
                    tracing::error!("Flow process state recovery failed: {e:?}");
                    println!("Flow process state recovery failed: {e:?}");
                    return Err(e);
                }
            }
        } else {
            tracing::info!("Flow process states found, skipping recovery");
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
