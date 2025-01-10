// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::component;
use internal_error::ResultIntoInternal;
use kamu_flow_system::{Flow, FlowEventStore, FlowID, FlowStateStream};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowStateHelper {
    flow_event_store: Arc<dyn FlowEventStore>,
}

#[component(pub)]
impl FlowStateHelper {
    pub(crate) fn new(flow_event_store: Arc<dyn FlowEventStore>) -> Self {
        Self { flow_event_store }
    }

    pub fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream {
        Box::pin(async_stream::try_stream! {
            // 32-items batching will give a performance boost,
            // but queries for long-lived datasets should not bee too heavy.
            // This number was chosen without any performance measurements. Subject of change.
            let chunk_size = 32;
            for chunk in flow_ids.chunks(chunk_size) {
                let flows = Flow::load_multi(
                    chunk.to_vec(),
                    self.flow_event_store.as_ref()
                ).await.int_err()?;
                for flow in flows {
                    yield flow.int_err()?.into();
                }
            }
        })
    }
}
