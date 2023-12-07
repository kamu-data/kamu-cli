// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_dataset_update_flow::*;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct SystemFlowConfigurationEventStoreInMem {
    inner: EventStoreInMemory<
        SystemFlowConfigurationState,
        EventStoreStateImpl<SystemFlowConfigurationState>,
    >,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SystemFlowConfigurationEventStore)]
#[scope(Singleton)]
impl SystemFlowConfigurationEventStoreInMem {
    pub fn new() -> Self {
        Self {
            inner: EventStoreInMemory::new(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<SystemFlowConfigurationState> for SystemFlowConfigurationEventStoreInMem {
    async fn len(&self) -> Result<usize, InternalError> {
        self.inner.len().await
    }

    fn get_events<'a>(
        &'a self,
        query: &SystemFlowKey,
        opts: GetEventsOpts,
    ) -> EventStream<'a, SystemFlowConfigurationEvent> {
        self.inner.get_events(query, opts)
    }

    async fn save_events(
        &self,
        query: &SystemFlowKey,
        events: Vec<SystemFlowConfigurationEvent>,
    ) -> Result<EventID, SaveEventsError> {
        self.inner.save_events(query, events).await
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

impl SystemFlowConfigurationEventStore for SystemFlowConfigurationEventStoreInMem {}

/////////////////////////////////////////////////////////////////////////////////////////
