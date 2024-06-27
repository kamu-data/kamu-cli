// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Builder, Catalog};
use internal_error::InternalError;

use crate::{AsyncEventHandler, EventHandler};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EventBus {
    catalog: Arc<Catalog>,
}

#[dill::component(pub)]
impl EventBus {
    pub fn new(catalog: Arc<Catalog>) -> EventBus {
        Self { catalog }
    }

    pub async fn dispatch_event<TEvent>(&self, event: TEvent) -> Result<(), InternalError>
    where
        TEvent: 'static + Clone,
    {
        self.sync_dispatch(&event)?;
        self.async_dispatch(&event).await?;

        Ok(())
    }

    fn sync_dispatch<TEvent: 'static + Clone>(&self, event: &TEvent) -> Result<(), InternalError> {
        let builders = self.catalog.builders_for::<dyn EventHandler<TEvent>>();

        for b in builders {
            tracing::debug!(
                handler = b.instance_type_name(),
                event = std::any::type_name::<TEvent>(),
                "Dispatching event to sync handler"
            );
            let inst = b.get(&self.catalog).unwrap();
            inst.handle(event)?;
        }

        Ok(())
    }

    async fn async_dispatch<TEvent: 'static + Clone>(
        &self,
        event: &TEvent,
    ) -> Result<(), InternalError> {
        let builders = self.catalog.builders_for::<dyn AsyncEventHandler<TEvent>>();

        let mut handlers = Vec::new();
        for b in builders {
            tracing::debug!(
                handler = b.instance_type_name(),
                event = std::any::type_name::<TEvent>(),
                "Dispatching event to async handler"
            );
            let handler = b.get(&self.catalog).unwrap();
            handlers.push(handler);
        }

        let futures: Vec<_> = handlers
            .iter()
            .map(|handler| handler.handle(event))
            .collect();

        let results = futures::future::join_all(futures).await;
        results.into_iter().try_for_each(|res| res)?;

        Ok(())
    }
}
