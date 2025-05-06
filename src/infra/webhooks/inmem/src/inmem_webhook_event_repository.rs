// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use database_common::PaginationOpts;
use dill::*;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWebhookEventRepository {
    state: Arc<RwLock<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    webhook_events: Vec<WebhookEvent>,
    webhook_events_by_id: HashMap<WebhookEventId, WebhookEvent>,
}

impl State {
    fn new() -> Self {
        Self::default()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn WebhookEventRepository)]
#[scope(Singleton)]
impl InMemoryWebhookEventRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookEventRepository for InMemoryWebhookEventRepository {
    async fn create_event(&self, event: &WebhookEvent) -> Result<(), CreateWebhookEventError> {
        let mut write_guard = self.state.write().unwrap();

        // Check against duplicates
        if write_guard.webhook_events_by_id.contains_key(&event.id) {
            return Err(CreateWebhookEventError::DuplicateId(
                WebhookEventDuplicateIdError { event_id: event.id },
            ));
        }

        // Save webhook event record
        write_guard.webhook_events.push(event.clone());
        write_guard
            .webhook_events_by_id
            .insert(event.id, event.clone());

        Ok(())
    }

    async fn get_event_by_id(
        &self,
        event_id: WebhookEventId,
    ) -> Result<WebhookEvent, GetWebhookEventError> {
        let guard = self.state.read().unwrap();

        let event = guard
            .webhook_events_by_id
            .get(&event_id)
            .cloned()
            .ok_or_else(|| {
                GetWebhookEventError::NotFound(WebhookEventNotFoundError { event_id })
            })?;

        Ok(event)
    }

    async fn list_recent_events(
        &self,
        pagination: PaginationOpts,
    ) -> Result<Vec<WebhookEvent>, ListRecentWebhookEventsError> {
        let guard = self.state.read().unwrap();

        let events = guard
            .webhook_events
            .iter()
            .rev()
            .skip(pagination.offset)
            .take(pagination.limit)
            .cloned()
            .collect();

        Ok(events)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
