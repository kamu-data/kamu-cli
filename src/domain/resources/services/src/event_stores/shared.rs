// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use chrono::{DateTime, Utc};
use event_sourcing::{
    EventID,
    EventStream,
    GetEventsError,
    GetEventsOpts,
    Projection,
    SaveEventsError,
};
use internal_error::{InternalError, ResultIntoInternal};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;

use crate::domain::{
    ReconcilableResourceEvent,
    ResourceDescriptorProvider,
    ResourceID,
    ResourceRawEvent,
    ResourceRawEventQuery,
    ResourceRawEventStore,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceBridgeEvent {
    fn resource_id(&self) -> &ResourceID;
    fn event_time(&self) -> DateTime<Utc>;
    fn typename(&self) -> &'static str;
}

impl<TSpec, TSuccess, TFailureDetails> ResourceBridgeEvent
    for ReconcilableResourceEvent<TSpec, TSuccess, TFailureDetails>
{
    fn resource_id(&self) -> &ResourceID {
        Self::resource_id(self)
    }

    fn event_time(&self) -> DateTime<Utc> {
        Self::event_time(self)
    }

    fn typename(&self) -> &'static str {
        Self::typename(self)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RawResourceEventStoreBridge<TResource, TState, TEvent>
where
    TResource: ResourceDescriptorProvider,
    TState: Projection<Query = ResourceID, Event = TEvent>,
    TEvent: ResourceBridgeEvent + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    _phantom: PhantomData<(TResource, TState, TEvent)>,
}

impl<TResource, TState, TEvent> RawResourceEventStoreBridge<TResource, TState, TEvent>
where
    TResource: ResourceDescriptorProvider,
    TState: Projection<Query = ResourceID, Event = TEvent>,
    TEvent: ResourceBridgeEvent + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    fn make_raw_query(resource_id: &ResourceID) -> ResourceRawEventQuery {
        ResourceRawEventQuery {
            kind: TResource::DESCRIPTOR.resource_type.to_string(),
            id: *resource_id,
        }
    }

    fn decode_raw_event(raw: ResourceRawEvent) -> Result<(EventID, TEvent), InternalError> {
        if raw.query.kind != TResource::DESCRIPTOR.resource_type {
            return InternalError::bail(format!(
                "Unexpected resource kind in resource event store bridge: expected='{}', \
                 actual='{}'",
                TResource::DESCRIPTOR.resource_type,
                raw.query.kind
            ));
        }

        let event_id = raw.event_id;
        let event_type = raw.event_type.clone();
        let event: TEvent = serde_json::from_value(raw.payload).context_int_err(format!(
            "Failed to deserialize resource event: event_id={event_id}"
        ))?;

        let decoded_event_type = event.typename();
        if event_type != decoded_event_type {
            return InternalError::bail(format!(
                "Unexpected resource event type: event_id={event_id}, \
                 expected='{decoded_event_type}', actual='{event_type}'"
            ));
        }

        Ok((event_id, event))
    }

    fn encode_raw_event(
        resource_id: &ResourceID,
        event: &TEvent,
    ) -> Result<ResourceRawEvent, InternalError> {
        if event.resource_id() != resource_id {
            return InternalError::bail(format!(
                "Resource event id does not match save query: expected='{resource_id}', \
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
                "Failed to serialize resource event: resource_id={resource_id}"
            ))?,
        })
    }

    pub async fn total_events_stored(
        raw_event_store: &dyn ResourceRawEventStore,
    ) -> Result<usize, InternalError> {
        raw_event_store
            .total_events_stored_by_kind(TResource::DESCRIPTOR.resource_type)
            .await
    }

    pub fn get_all_events(
        raw_event_store: &dyn ResourceRawEventStore,
        opts: GetEventsOpts,
    ) -> EventStream<'_, TEvent> {
        Box::pin(
            raw_event_store
                .get_all_events_by_kind(TResource::DESCRIPTOR.resource_type, opts)
                .map(|result| {
                    result.and_then(|(_, raw_event)| {
                        Self::decode_raw_event(raw_event).map_err(GetEventsError::Internal)
                    })
                }),
        )
    }

    pub fn get_events<'a>(
        raw_event_store: &'a dyn ResourceRawEventStore,
        query: &ResourceID,
        opts: GetEventsOpts,
    ) -> EventStream<'a, TEvent> {
        let raw_query = Self::make_raw_query(query);

        Box::pin(raw_event_store.get_events(&raw_query, opts).map(|result| {
            result.and_then(|(_, raw_event)| {
                Self::decode_raw_event(raw_event).map_err(GetEventsError::Internal)
            })
        }))
    }

    pub async fn save_events(
        raw_event_store: &dyn ResourceRawEventStore,
        query: &ResourceID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<TEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let raw_query = Self::make_raw_query(query);
        let raw_events = events
            .iter()
            .map(|event| Self::encode_raw_event(query, event))
            .collect::<Result<Vec<_>, _>>()
            .map_err(SaveEventsError::Internal)?;

        raw_event_store
            .save_events(&raw_query, maybe_prev_stored_event_id, raw_events)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_event_store_bridge {
    (
        bridge = $bridge:ident,
        trait = $store_trait:ident,
        resource = $resource:ty,
        state = $state:ty,
        event = $event:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $store_trait)]
        pub struct $bridge {
            raw_event_store: std::sync::Arc<dyn $crate::domain::ResourceRawEventStore>,
        }

        #[async_trait::async_trait]
        impl event_sourcing::EventStore<$state> for $bridge {
            async fn total_events_stored(&self) -> Result<usize, internal_error::InternalError> {
                $crate::event_stores::shared::RawResourceEventStoreBridge::<
                                            $resource,
                                            $state,
                                            $event,
                                        >::total_events_stored(self.raw_event_store.as_ref())
                                        .await
            }

            fn get_all_events(
                &self,
                opts: event_sourcing::GetEventsOpts,
            ) -> event_sourcing::EventStream<'_, $event> {
                $crate::event_stores::shared::RawResourceEventStoreBridge::<
                                            $resource,
                                            $state,
                                            $event,
                                        >::get_all_events(self.raw_event_store.as_ref(), opts)
            }

            fn get_events(
                &self,
                query: &$crate::domain::ResourceID,
                opts: event_sourcing::GetEventsOpts,
            ) -> event_sourcing::EventStream<'_, $event> {
                $crate::event_stores::shared::RawResourceEventStoreBridge::<
                                            $resource,
                                            $state,
                                            $event,
                                        >::get_events(self.raw_event_store.as_ref(), query, opts)
            }

            async fn save_events(
                &self,
                query: &$crate::domain::ResourceID,
                maybe_prev_stored_event_id: Option<event_sourcing::EventID>,
                events: Vec<$event>,
            ) -> Result<event_sourcing::EventID, event_sourcing::SaveEventsError> {
                $crate::event_stores::shared::RawResourceEventStoreBridge::<
                                            $resource,
                                            $state,
                                            $event,
                                        >::save_events(
                                            self.raw_event_store.as_ref(),
                                            query,
                                            maybe_prev_stored_event_id,
                                            events,
                                        )
                                        .await
            }
        }

        impl $store_trait for $bridge {}
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
