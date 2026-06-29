// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::marker::PhantomData;

use event_sourcing::{
    EventID,
    EventStream,
    GetEventsError,
    GetEventsOpts,
    MultiEventStream,
    Projection,
    SaveEventsError,
    SaveEventsItem,
};
use internal_error::{InternalError, ResultIntoInternal};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio_stream::StreamExt;

use crate::ResourceBridgeEvent;
use crate::domain::{
    ResourceDescriptorProvider,
    ResourceID,
    ResourceRawEvent,
    ResourceRawEventQuery,
    ResourceRawEventStore,
};

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
    fn make_raw_query(id: &ResourceID) -> ResourceRawEventQuery {
        ResourceRawEventQuery {
            kind: TResource::DESCRIPTOR.resource_type.to_string(),
            id: *id,
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
        id: &ResourceID,
        event: &TEvent,
    ) -> Result<ResourceRawEvent, InternalError> {
        if event.id() != id {
            return InternalError::bail(format!(
                "Resource event id does not match save query: expected='{id}', actual='{}'",
                event.id()
            ));
        }

        Ok(ResourceRawEvent {
            event_id: EventID::new(0),
            query: Self::make_raw_query(id),
            event_time: event.event_time(),
            event_type: event.typename().to_string(),
            payload: serde_json::to_value(event)
                .context_int_err(format!("Failed to serialize resource event: id={id}"))?,
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

    pub fn get_events_multi<'a>(
        raw_event_store: &'a dyn ResourceRawEventStore,
        queries: &[ResourceID],
    ) -> MultiEventStream<'a, ResourceID, TEvent> {
        let raw_queries = queries.iter().map(Self::make_raw_query).collect::<Vec<_>>();

        Box::pin(
            raw_event_store
                .get_events_multi(&raw_queries)
                .map(|result| {
                    result.and_then(|(raw_query, event_id, raw_event)| {
                        let (_, event) =
                            Self::decode_raw_event(raw_event).map_err(GetEventsError::Internal)?;
                        Ok((raw_query.id, event_id, event))
                    })
                }),
        )
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

    pub async fn save_events_multi(
        raw_event_store: &dyn ResourceRawEventStore,
        items: Vec<SaveEventsItem<ResourceID, TEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        let raw_items = items
            .into_iter()
            .map(|item| {
                let raw_query = Self::make_raw_query(&item.query);
                let raw_events = item
                    .events
                    .iter()
                    .map(|event| Self::encode_raw_event(&item.query, event))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(SaveEventsError::Internal)?;

                Ok(SaveEventsItem {
                    query: raw_query,
                    maybe_prev_stored_event_id: item.maybe_prev_stored_event_id,
                    events: raw_events,
                })
            })
            .collect::<Result<Vec<_>, SaveEventsError>>()?;

        raw_event_store.save_events_multi(raw_items).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_event_store_bridge {
    (
        bridge = $bridge:ident,
        store = $store:path,
        resource = $resource:ty,
        state = $state:ty,
        event = $event:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $store)]
        pub struct $bridge {
            raw_event_store: std::sync::Arc<dyn kamu_resources::ResourceRawEventStore>,
        }

        #[async_trait::async_trait]
        impl event_sourcing::EventStore<$state> for $bridge {
            async fn total_events_stored(&self) -> Result<usize, internal_error::InternalError> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::total_events_stored(
                    self.raw_event_store.as_ref()
                )
                .await
            }

            fn get_all_events(
                &self,
                opts: event_sourcing::GetEventsOpts,
            ) -> event_sourcing::EventStream<'_, $event> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::get_all_events(
                    self.raw_event_store.as_ref(),
                    opts
                )
            }

            fn get_events(
                &self,
                query: &kamu_resources::ResourceID,
                opts: event_sourcing::GetEventsOpts,
            ) -> event_sourcing::EventStream<'_, $event> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::get_events(
                    self.raw_event_store.as_ref(),
                    query,
                    opts
                )
            }

            fn get_events_multi(
                &self,
                queries: &[kamu_resources::ResourceID],
            ) -> event_sourcing::MultiEventStream<'_, kamu_resources::ResourceID, $event> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::get_events_multi(
                    self.raw_event_store.as_ref(),
                    queries,
                )
            }

            async fn save_events(
                &self,
                query: &kamu_resources::ResourceID,
                maybe_prev_stored_event_id: Option<event_sourcing::EventID>,
                events: Vec<$event>,
            ) -> Result<event_sourcing::EventID, event_sourcing::SaveEventsError> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::save_events(
                    self.raw_event_store.as_ref(),
                    query,
                    maybe_prev_stored_event_id,
                    events,
                )
                .await
            }

            async fn save_events_multi(
                &self,
                items: Vec<event_sourcing::SaveEventsItem<kamu_resources::ResourceID, $event>>,
            ) -> Result<Vec<event_sourcing::EventID>, event_sourcing::SaveEventsError> {
                $crate::RawResourceEventStoreBridge::<$resource, $state, $event>::save_events_multi(
                    self.raw_event_store.as_ref(),
                    items,
                )
                .await
            }
        }

        #[async_trait::async_trait]
        impl $store for $bridge {}
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
