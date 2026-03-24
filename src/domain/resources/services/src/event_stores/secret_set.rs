// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use event_sourcing::{
    EventID,
    EventStore,
    EventStream,
    GetEventsError,
    GetEventsOpts,
    SaveEventsError,
};
use internal_error::{InternalError, ResultIntoInternal};
use tokio_stream::StreamExt;

use crate::domain::{
    ResourceDescriptorProvider,
    ResourceID,
    ResourceRawEvent,
    ResourceRawEventQuery,
    ResourceRawEventStore,
    SecretSetEvent,
    SecretSetEventStore,
    SecretSetResource,
    SecretSetState,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn SecretSetEventStore)]
pub struct SecretSetEventStoreBridge {
    raw_event_store: Arc<dyn ResourceRawEventStore>,
}

impl SecretSetEventStoreBridge {
    fn make_raw_query(resource_id: &ResourceID) -> ResourceRawEventQuery {
        ResourceRawEventQuery {
            kind: SecretSetResource::DESCRIPTOR.resource_type.to_string(),
            id: *resource_id,
        }
    }

    fn decode_raw_event(raw: ResourceRawEvent) -> Result<(EventID, SecretSetEvent), InternalError> {
        if raw.query.kind != SecretSetResource::DESCRIPTOR.resource_type {
            return InternalError::bail(format!(
                "Unexpected resource kind in SecretSet event store bridge: expected='{}', \
                 actual='{}'",
                SecretSetResource::DESCRIPTOR.resource_type,
                raw.query.kind
            ));
        }

        let event_id = raw.event_id;
        let event_type = raw.event_type.clone();
        let event: SecretSetEvent = serde_json::from_value(raw.payload).context_int_err(
            format!("Failed to deserialize SecretSet event: event_id={event_id}"),
        )?;

        let decoded_event_type = event.typename();
        if event_type != decoded_event_type {
            return InternalError::bail(format!(
                "Unexpected SecretSet event type: event_id={event_id}, \
                 expected='{decoded_event_type}', actual='{event_type}'"
            ));
        }

        Ok((event_id, event))
    }

    fn encode_raw_event(
        resource_id: &ResourceID,
        event: &SecretSetEvent,
    ) -> Result<ResourceRawEvent, InternalError> {
        if event.resource_id() != resource_id {
            return InternalError::bail(format!(
                "SecretSet event resource id does not match save query: expected='{resource_id}', \
                 actual='{}'",
                event.resource_id()
            ));
        }

        Ok(ResourceRawEvent {
            event_id: EventID::new(0),
            query: Self::make_raw_query(resource_id),
            event_time: event.event_time(),
            event_type: event.typename().to_string(),
            payload: serde_json::to_value(event).context_int_err(format!(
                "Failed to serialize SecretSet event: resource_id={resource_id}"
            ))?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<SecretSetState> for SecretSetEventStoreBridge {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        self.raw_event_store
            .total_events_stored_by_kind(SecretSetResource::DESCRIPTOR.resource_type)
            .await
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, SecretSetEvent> {
        Box::pin(
            self.raw_event_store
                .get_all_events_by_kind(SecretSetResource::DESCRIPTOR.resource_type, opts)
                .map(|result| {
                    result.and_then(|(_, raw_event)| {
                        Self::decode_raw_event(raw_event).map_err(GetEventsError::Internal)
                    })
                }),
        )
    }

    fn get_events(
        &self,
        query: &ResourceID,
        opts: GetEventsOpts,
    ) -> EventStream<'_, SecretSetEvent> {
        let raw_query = Self::make_raw_query(query);

        Box::pin(
            self.raw_event_store
                .get_events(&raw_query, opts)
                .map(|result| {
                    result.and_then(|(_, raw_event)| {
                        Self::decode_raw_event(raw_event).map_err(GetEventsError::Internal)
                    })
                }),
        )
    }

    async fn save_events(
        &self,
        query: &ResourceID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<SecretSetEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let raw_query = Self::make_raw_query(query);
        let raw_events = events
            .iter()
            .map(|event| Self::encode_raw_event(query, event))
            .collect::<Result<Vec<_>, _>>()
            .map_err(SaveEventsError::Internal)?;

        self.raw_event_store
            .save_events(&raw_query, maybe_prev_stored_event_id, raw_events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SecretSetEventStore for SecretSetEventStoreBridge {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
