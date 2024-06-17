// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use dill::*;
use event_bus::{AsyncEventHandler, EventBus, EventHandler};
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Copy, Clone)]
struct Event {}

////////////////////////////////////////////////////////////////////////////////////////

struct TestSyncHandler {
    invoked: Arc<Mutex<bool>>,
}

#[component(pub)]
#[interface(dyn EventHandler<Event>)]
#[scope(Singleton)]
impl TestSyncHandler {
    fn new() -> Self {
        Self {
            invoked: Arc::new(Mutex::new(false)),
        }
    }

    fn was_invoked(&self) -> bool {
        *self.invoked.lock().unwrap()
    }
}

impl EventHandler<Event> for TestSyncHandler {
    fn handle(&self, _: &Event) -> Result<(), InternalError> {
        let mut invoked = self.invoked.lock().unwrap();
        *invoked = true;
        Ok(())
    }
}

#[test_log::test(tokio::test)]
async fn test_bus_sync_handler() {
    let catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<TestSyncHandler>()
        .build();
    let event_bus = catalog.get_one::<EventBus>().unwrap();

    event_bus.dispatch_event(Event {}).await.unwrap();

    let handler = catalog.get_one::<TestSyncHandler>().unwrap();
    assert!(handler.was_invoked());
}

////////////////////////////////////////////////////////////////////////////////////////

struct TestAsyncHandler {
    invoked: Arc<Mutex<bool>>,
}

#[component(pub)]
#[interface(dyn AsyncEventHandler<Event>)]
#[scope(Singleton)]
impl TestAsyncHandler {
    fn new() -> Self {
        Self {
            invoked: Arc::new(Mutex::new(false)),
        }
    }

    fn was_invoked(&self) -> bool {
        *self.invoked.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl AsyncEventHandler<Event> for TestAsyncHandler {
    async fn handle(&self, _: &Event) -> Result<(), InternalError> {
        let mut invoked = self.invoked.lock().unwrap();
        *invoked = true;
        Ok(())
    }
}

#[test_log::test(tokio::test)]
async fn test_bus_async_handler() {
    let catalog = dill::CatalogBuilder::new()
        .add::<EventBus>()
        .add::<TestAsyncHandler>()
        .build();
    let event_bus = catalog.get_one::<EventBus>().unwrap();

    event_bus.dispatch_event(Event {}).await.unwrap();

    let handler = catalog.get_one::<TestAsyncHandler>().unwrap();
    assert!(handler.was_invoked());
}

////////////////////////////////////////////////////////////////////////////////////////
