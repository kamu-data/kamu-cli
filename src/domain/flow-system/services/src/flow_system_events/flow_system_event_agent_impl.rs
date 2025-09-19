// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_utils::BackgroundAgent;
use kamu_flow_system::{FlowSystemEventAgent, FlowSystemEventAgentConfig, FlowSystemEventStore};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn BackgroundAgent)]
#[dill::interface(dyn FlowSystemEventAgent)]
#[dill::scope(dill::Singleton)]
pub struct FlowSystemEventAgentImpl {
    flow_system_event_store: Arc<dyn FlowSystemEventStore>,
    agent_config: Arc<FlowSystemEventAgentConfig>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl BackgroundAgent for FlowSystemEventAgentImpl {
    fn agent_name(&self) -> &'static str {
        "dev.kamu.domain.flow-system.FlowSystemEventAgent"
    }

    async fn run(&self) -> Result<(), internal_error::InternalError> {
        let mut backoff = self.agent_config.min_listening_timeout;

        loop {
            println!(
                "Starting iteration with backoff {:?}, current time {:?}",
                backoff,
                std::time::Instant::now()
            );

            // 1) Wait for push or timeout.
            self.flow_system_event_store.wait_wake(backoff).await?;
            println!("Woke up at {:?}", std::time::Instant::now());

            let made_progress = false;

            // 2) For each projector, drain until no work.
            /*for p in &self.projectors {
                loop {
                    let mut tx = self.store.begin(p.name()).await?;

                    let batch = tx.fetch_next_batch(self.batch_size).await?;
                    if batch.is_empty() {
                        tx.rollback().await.ok();
                        break;
                    }

                    // Optional: prefilter in memory to avoid useless writes
                    let ids: Vec<i64> = batch.iter().map(|e| e.id).collect();

                    for e in &batch {
                        if p.interested(e) {
                            p.apply(tx.as_mut(), e).await?;
                        }
                    }

                    // mark & commit atomically with view writes
                    tx.mark_applied(&ids).await?;
                    tx.commit().await?;

                    made_progress = true;
                }
            }*/

            // 3) Adjust backoff
            if made_progress {
                backoff = self.agent_config.min_listening_timeout;
            } else {
                backoff = std::cmp::min(backoff * 2, self.agent_config.max_listening_timeout);
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl FlowSystemEventAgent for FlowSystemEventAgentImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
