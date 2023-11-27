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

use crate::{AsyncEventHandler, EventHandler};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EventBus {
    catalog: Arc<Catalog>,
    state: Mutex<State>,
}

#[component(pub)]
#[scope(Singleton)]
impl EventBus {
    pub fn new(catalog: Arc<Catalog>) -> EventBus {
        Self {
            catalog,
            state: Mutex::new(State::new()),
        }
    }

    pub fn subscribe_async_closure<TEvent, H, HFut>(&self, callback: H)
    where
        TEvent: 'static + Clone,
        H: Fn(Arc<TEvent>) -> HFut + Send + Sync + 'static,
        HFut: std::future::Future<Output = Result<(), InternalError>> + Send + 'static,
    {
        let mut state = self.state.lock().unwrap();

        let event_handlers = state.take_closure_handlers_for::<TEvent>();

        let async_closure_handler = Arc::new(
            move |event: Arc<TEvent>|
                  -> Pin<Box<dyn Future<Output = Result<(), InternalError>> + Send>> {
                Box::pin(callback(event))
            },
        );

        event_handlers.0.push(async_closure_handler);
    }

    pub async fn dispatch_event<TEvent>(&self, event: TEvent) -> Result<(), InternalError>
    where
        TEvent: 'static + Clone,
    {
        self.sync_dispatch(&event)?;
        self.async_dispatch(&event).await?;
        self.async_closures_dispatch(&event).await?;

        Ok(())
    }

    fn sync_dispatch<TEvent: 'static + Clone>(&self, event: &TEvent) -> Result<(), InternalError> {
        let sync_handlers = self
            .catalog
            .get::<dill::AllOf<dyn EventHandler<TEvent>>>()
            .unwrap();

        for sync_handler in sync_handlers {
            sync_handler.handle(event)?;
        }

        Ok(())
    }

    async fn async_dispatch<TEvent: 'static + Clone>(
        &self,
        event: &TEvent,
    ) -> Result<(), InternalError> {
        let async_handlers = self
            .catalog
            .get::<dill::AllOf<dyn AsyncEventHandler<TEvent>>>()
            .unwrap();

        let async_handler_futures: Vec<_> = async_handlers
            .iter()
            .map(|handler| handler.handle(event))
            .collect();

        futures::future::try_join_all(async_handler_futures).await?;

        Ok(())
    }

    async fn async_closures_dispatch<TEvent: 'static + Clone>(
        &self,
        event: &TEvent,
    ) -> Result<(), InternalError> {
        let maybe_closure_handlers: Option<EventClosureHandlers<TEvent>> = {
            let state = self.state.lock().unwrap();
            let maybe_event_handlers = state.get_closure_handlers_for::<TEvent>();
            maybe_event_handlers.map(|handlers| handlers.clone())
        };

        if let Some(closure_handlers) = maybe_closure_handlers {
            let event_arc = Arc::new(event.clone());
            let closure_handler_futures: Vec<_> = closure_handlers
                .0
                .iter()
                .map(|handler| (*handler).call((event_arc.clone(),)))
                .collect();

            futures::future::try_join_all(closure_handler_futures).await?;
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

struct State {
    closure_handlers_by_event_type: HashMap<TypeId, Box<dyn Any + Send + Sync + 'static>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            closure_handlers_by_event_type: HashMap::new(),
        }
    }

    pub fn get_closure_handlers_for<'a, TEvent: 'static + Clone>(
        &'a self,
    ) -> Option<&'a EventClosureHandlers<TEvent>> {
        if let Some(event_handlers) = self
            .closure_handlers_by_event_type
            .get(&TypeId::of::<TEvent>())
        {
            let event_handlers = event_handlers
                .downcast_ref::<EventClosureHandlers<TEvent>>()
                .unwrap();
            Some(event_handlers)
        } else {
            None
        }
    }

    pub fn take_closure_handlers_for<TEvent: 'static + Clone>(
        &mut self,
    ) -> &mut EventClosureHandlers<TEvent> {
        self.closure_handlers_by_event_type
            .entry(TypeId::of::<TEvent>())
            .or_insert(Box::new(EventClosureHandlers::<TEvent>::default()))
            .downcast_mut::<EventClosureHandlers<TEvent>>()
            .unwrap()
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

type AsyncEventClosure<TEvent> = Arc<
    dyn Fn(Arc<TEvent>) -> Pin<Box<dyn Future<Output = Result<(), InternalError>> + Send>>
        + Send
        + Sync,
>;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
struct EventClosureHandlers<TEvent>(Vec<AsyncEventClosure<TEvent>>);

impl<TEvent> Default for EventClosureHandlers<TEvent> {
    fn default() -> Self {
        Self(vec![])
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
