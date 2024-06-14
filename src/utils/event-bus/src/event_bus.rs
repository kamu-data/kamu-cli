// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, Catalog};
use internal_error::InternalError;

use crate::{AsyncEventHandler, EventHandler};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EventBus {
    catalog: Arc<Catalog>,
}

#[component(pub)]
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

        let results = futures::future::join_all(async_handler_futures).await;
        results.into_iter().try_for_each(|res| res)?;

        Ok(())
    }
}
