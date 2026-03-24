// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use event_sourcing::{
    EventID,
    EventStore,
    EventStream,
    GetEventsError,
    GetEventsOpts,
    SaveEventsError,
};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceRawEvent,
    ResourceRawEventProjection,
    ResourceRawEventQuery,
    ResourceRawEventStore,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceRawEventStore)]
pub struct SqliteResourceRawEventStore {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SqliteResourceRawEventStore {
    async fn save_event_rows(
        connection_mut: &mut sqlx::SqliteConnection,
        query: &ResourceRawEventQuery,
        events: &[ResourceRawEvent],
    ) -> Result<EventID, SaveEventsError> {
        let mut last_event_id = None;

        for event in events {
            let inserted = sqlx::query_scalar!(
                r#"
                INSERT INTO resource_events (
                    resource_id,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload
                )
                VALUES ($1, $2, $3, $4, $5)

                RETURNING event_id
                "#,
                query.id,
                query.kind,
                event.event_time,
                event.event_type,
                event.payload,
            )
            .fetch_one(&mut *connection_mut)
            .await
            .int_err()
            .map_err(SaveEventsError::Internal)?;

            last_event_id = Some(EventID::new(inserted));
        }

        Ok(last_event_id.expect("events is not empty"))
    }

    async fn get_last_event_id(
        connection_mut: &mut sqlx::SqliteConnection,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<EventID>, InternalError> {
        let last_event_id = sqlx::query_scalar!(
            r#"
            SELECT event_id
            FROM resource_events
            WHERE resource_id = $1
              AND resource_kind = $2
            ORDER BY event_id DESC
            LIMIT 1
            "#,
            query.id,
            query.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map(|maybe_event_id| maybe_event_id.map(EventID::new))?;

        Ok(last_event_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<ResourceRawEventProjection> for SqliteResourceRawEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let events_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!: i64"
            FROM resource_events
            "#
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(events_count).unwrap())
    }

    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, ResourceRawEvent> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;
            let maybe_from_id = opts.from.map(EventID::into_inner);
            let maybe_to_id = opts.to.map(EventID::into_inner);

            #[derive(Debug, sqlx::FromRow)]
            struct EventRow {
                event_id: i64,
                resource_id: uuid::Uuid,
                kind: String,
                event_time: DateTime<Utc>,
                event_type: String,
                payload: serde_json::Value,
            }

            let mut rows = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id,
                    resource_id as "resource_id: _",
                    resource_kind as "kind!",
                    event_time as "event_time: DateTime<Utc>",
                    event_type,
                    event_payload as "payload: serde_json::Value"
                FROM resource_events
                WHERE ($1 IS NULL OR event_id > $1)
                  AND ($2 IS NULL OR event_id <= $2)
                ORDER BY event_id
                "#,
                maybe_from_id,
                maybe_to_id,
            )
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                yield Ok((event_id, ResourceRawEvent {
                    event_id,
                    query: ResourceRawEventQuery {
                        kind: row.kind,
                        id: row.resource_id,
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                }));
            }
        })
    }

    fn get_events(
        &self,
        query: &ResourceRawEventQuery,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        let query = query.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;
            let maybe_from_id = opts.from.map(EventID::into_inner);
            let maybe_to_id = opts.to.map(EventID::into_inner);

            #[derive(Debug, sqlx::FromRow)]
            struct EventRow {
                event_id: i64,
                resource_id: uuid::Uuid,
                kind: String,
                event_time: DateTime<Utc>,
                event_type: String,
                payload: serde_json::Value,
            }

            let mut rows = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id,
                    resource_id as "resource_id: _",
                    resource_kind as "kind!",
                    event_time as "event_time: DateTime<Utc>",
                    event_type,
                    event_payload as "payload: serde_json::Value"
                FROM resource_events
                WHERE resource_id = $1
                  AND resource_kind = $2
                  AND ($3 IS NULL OR event_id > $3)
                  AND ($4 IS NULL OR event_id <= $4)
                ORDER BY event_id
                "#,
                query.id,
                query.kind,
                maybe_from_id,
                maybe_to_id,
            )
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                yield Ok((event_id, ResourceRawEvent {
                    event_id,
                    query: ResourceRawEventQuery {
                        kind: row.kind,
                        id: row.resource_id,
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                }));
            }
        })
    }

    async fn save_events(
        &self,
        query: &ResourceRawEventQuery,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<ResourceRawEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        let actual_last_event_id = Self::get_last_event_id(connection_mut, query)
            .await
            .map_err(SaveEventsError::Internal)?;

        if actual_last_event_id != maybe_prev_stored_event_id {
            return Err(SaveEventsError::concurrent_modification());
        }

        Self::save_event_rows(connection_mut, query, &events).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ResourceRawEventStore for SqliteResourceRawEventStore {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
