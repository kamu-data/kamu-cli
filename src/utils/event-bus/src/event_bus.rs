// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use dill::{component, scope, Catalog, Singleton};
use internal_error::InternalError;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EventBus {
    catalog: Arc<Catalog>,
    event_handlers_map: Arc<Mutex<HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>>>,
}

#[component(pub)]
#[scope(Singleton)]
impl EventBus {
    pub fn new(catalog: Arc<Catalog>) -> EventBus {
        Self {
            catalog,
            event_handlers_map: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn dispatch_event<TEvent: 'static + Clone>(
        &self,
        event: TEvent,
    ) -> Result<(), InternalError> {
        let handlers: Vec<_> = {
            let map = self.event_handlers_map.lock().unwrap();
            if let Some(handlers) = map.get(&TypeId::of::<TEvent>()) {
                let handlers = handlers.downcast_ref::<EventBusHandlers<TEvent>>().unwrap();
                handlers.0.iter().map(|h| h.clone()).collect()
            } else {
                Vec::new()
            }
        };

        for handler in handlers.iter() {
            (*handler)
                .call((self.catalog.clone(), event.clone()))
                .await?;
        }

        Ok(())
    }

    pub fn subscribe_event<TEvent, H, HFut>(&self, callback: H)
    where
        TEvent: 'static + Clone,
        H: Fn(Arc<Catalog>, TEvent) -> HFut + Send + Sync + 'static,
        HFut: std::future::Future<Output = Result<(), InternalError>> + Send + 'static,
    {
        let mut handler_map = self.event_handlers_map.lock().unwrap();

        let handlers = handler_map
            .entry(TypeId::of::<TEvent>())
            .or_insert(Box::new(EventBusHandlers::<TEvent>::default()))
            .downcast_mut::<EventBusHandlers<TEvent>>()
            .unwrap();

        let callback = Arc::new(
            move |catalog: Arc<Catalog>,
                  event: TEvent|
                  -> Pin<Box<dyn Future<Output = Result<(), InternalError>> + Send>> {
                Box::pin(callback(catalog, event))
            },
        );

        handlers.0.push(callback);
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub type EventCallback<TEvent> = Arc<
    dyn Fn(Arc<Catalog>, TEvent) -> Pin<Box<dyn Future<Output = Result<(), InternalError>> + Send>>
        + Send
        + Sync,
>;

/////////////////////////////////////////////////////////////////////////////////////////

struct EventBusHandlers<TEvent>(Vec<EventCallback<TEvent>>);

impl<TEvent> Default for EventBusHandlers<TEvent> {
    fn default() -> Self {
        Self(vec![])
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
