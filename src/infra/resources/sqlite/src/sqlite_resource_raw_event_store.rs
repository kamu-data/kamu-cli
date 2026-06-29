// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use event_sourcing::{
    EventID,
    EventStore,
    EventStream,
    GetEventsError,
    GetEventsOpts,
    MultiEventStream,
    SaveEventsError,
    SaveEventsItem,
};
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    ResourceID,
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

#[derive(Debug, sqlx::FromRow)]
struct EventRow {
    event_id: i64,
    id: uuid::Uuid,
    kind: String,
    event_time: DateTime<Utc>,
    event_type: String,
    payload: serde_json::Value,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SqliteResourceRawEventStore {
    // Sqlite has a limit of 999 parameters per query
    const MAX_EVENTS_PER_INSERT: usize = 100;

    async fn save_event_rows(
        connection_mut: &mut sqlx::SqliteConnection,
        query: &ResourceRawEventQuery,
        events: &[ResourceRawEvent],
    ) -> Result<EventID, SaveEventsError> {
        assert!(!events.is_empty());

        let mut last_event_id = None;
        for chunk in events.chunks(Self::MAX_EVENTS_PER_INSERT) {
            let rows = chunk
                .iter()
                .map(|event| (0, query, event))
                .collect::<Vec<_>>();

            let inserted_rows = Self::save_event_rows_chunk(connection_mut, &rows).await?;
            last_event_id = inserted_rows.last().map(|(_, event_id)| *event_id);
        }

        Ok(last_event_id.expect("events is not empty"))
    }

    async fn save_event_rows_multi(
        connection_mut: &mut sqlx::SqliteConnection,
        items: &[SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>],
    ) -> Result<Vec<EventID>, SaveEventsError> {
        let mut result = vec![None; items.len()];
        let mut rows = Vec::with_capacity(Self::MAX_EVENTS_PER_INSERT);

        for (item_index, item) in items.iter().enumerate() {
            for event in &item.events {
                rows.push((item_index, &item.query, event));

                if rows.len() == Self::MAX_EVENTS_PER_INSERT {
                    let inserted_rows = Self::save_event_rows_chunk(connection_mut, &rows).await?;
                    Self::update_multi_save_result(&mut result, inserted_rows);
                    rows.clear();
                }
            }
        }

        if !rows.is_empty() {
            let inserted_rows = Self::save_event_rows_chunk(connection_mut, &rows).await?;
            Self::update_multi_save_result(&mut result, inserted_rows);
        }

        Ok(result
            .into_iter()
            .map(|event_id| event_id.expect("Every item has events"))
            .collect())
    }

    async fn save_event_rows_chunk(
        connection_mut: &mut sqlx::SqliteConnection,
        rows: &[(usize, &ResourceRawEventQuery, &ResourceRawEvent)],
    ) -> Result<Vec<(usize, EventID)>, SaveEventsError> {
        assert!(!rows.is_empty());

        #[derive(Debug, sqlx::FromRow)]
        struct InsertedEventRow {
            event_id: i64,
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO resource_events (
                resource_id,
                resource_kind,
                event_time,
                event_type,
                event_payload
            )
            "#,
        );

        query_builder.push_values(rows, |mut b, row| {
            let (_, query, event) = *row;
            b.push_bind(*query.id.as_ref());
            b.push_bind(&query.kind);
            b.push_bind(event.event_time);
            b.push_bind(&event.event_type);
            b.push_bind(&event.payload);
        });

        query_builder.push(" RETURNING event_id");

        let inserted_rows = query_builder
            .build_query_as::<InsertedEventRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()
            .map_err(SaveEventsError::Internal)?;

        Ok(rows
            .iter()
            .zip(inserted_rows)
            .map(|((item_index, _, _), row)| (*item_index, EventID::new(row.event_id)))
            .collect())
    }

    fn update_multi_save_result(
        result: &mut [Option<EventID>],
        inserted_rows: Vec<(usize, EventID)>,
    ) {
        for (item_index, event_id) in inserted_rows {
            result[item_index] = Some(event_id);
        }
    }

    async fn get_last_event_id(
        connection_mut: &mut sqlx::SqliteConnection,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<EventID>, InternalError> {
        let query_id: &uuid::Uuid = query.id.as_ref();
        let last_event_id = sqlx::query_scalar!(
            r#"
            SELECT event_id
            FROM resource_events
            WHERE resource_id = $1
              AND resource_kind = $2
            ORDER BY event_id DESC
            LIMIT 1
            "#,
            query_id,
            query.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map(|maybe_event_id| maybe_event_id.map(EventID::new))?;

        Ok(last_event_id)
    }

    async fn get_last_event_ids(
        connection_mut: &mut sqlx::SqliteConnection,
        queries: &[ResourceRawEventQuery],
    ) -> Result<HashMap<ResourceRawEventQuery, EventID>, InternalError> {
        if queries.is_empty() {
            return Ok(HashMap::new());
        }

        #[derive(Debug, sqlx::FromRow)]
        struct LastEventRow {
            id: uuid::Uuid,
            kind: String,
            event_id: i64,
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT
                resource_id as id,
                resource_kind as kind,
                MAX(event_id) as event_id
            FROM resource_events
            WHERE
            "#,
        );

        for (i, query) in queries.iter().enumerate() {
            if i > 0 {
                query_builder.push(" OR ");
            }

            query_builder
                .push("(")
                .push("resource_id = ")
                .push_bind(*query.id.as_ref())
                .push(" AND resource_kind = ")
                .push_bind(&query.kind)
                .push(")");
        }

        query_builder.push(" GROUP BY resource_id, resource_kind");

        let rows = query_builder
            .build_query_as::<LastEventRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    ResourceRawEventQuery {
                        kind: row.kind,
                        id: ResourceID::new(row.id),
                    },
                    EventID::new(row.event_id),
                )
            })
            .collect())
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

            let mut rows = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id,
                    resource_id as "id: _",
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
                        id: ResourceID::new(row.id),
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
            let query_id: &uuid::Uuid = query.id.as_ref();

            let mut rows = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id,
                    resource_id as "id: _",
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
                query_id,
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
                        id: ResourceID::new(row.id),
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                }));
            }
        })
    }

    fn get_events_multi(
        &self,
        queries: &[ResourceRawEventQuery],
    ) -> MultiEventStream<'_, ResourceRawEventQuery, ResourceRawEvent> {
        if queries.is_empty() {
            return Box::pin(futures::stream::empty());
        }

        let queries = queries.to_vec();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                r#"
                SELECT
                    event_id,
                    resource_id as id,
                    resource_kind as kind,
                    event_time,
                    event_type,
                    event_payload as payload
                FROM resource_events
                WHERE
                "#,
            );

            for (i, query) in queries.iter().enumerate() {
                if i > 0 {
                    query_builder.push(" OR ");
                }

                query_builder
                    .push("(")
                    .push("resource_id = ")
                    .push_bind(*query.id.as_ref())
                    .push(" AND resource_kind = ")
                    .push_bind(&query.kind)
                    .push(")");
            }

            query_builder.push(" ORDER BY event_id");

            let mut rows = query_builder
                .build_query_as::<EventRow>()
                .fetch(connection_mut)
                .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                let query = ResourceRawEventQuery {
                    kind: row.kind,
                    id: ResourceID::new(row.id),
                };
                yield Ok((query.clone(), event_id, ResourceRawEvent {
                    event_id,
                    query,
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

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        event_sourcing::validate_multi_save_items(&items)?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr
            .connection_mut()
            .await
            .map_err(ErrorIntoInternal::int_err)?;

        let queries = items
            .iter()
            .map(|item| item.query.clone())
            .collect::<Vec<_>>();
        let actual_last_event_ids = Self::get_last_event_ids(connection_mut, &queries)
            .await
            .map_err(SaveEventsError::Internal)?;

        for item in &items {
            let actual_last_event_id = actual_last_event_ids.get(&item.query).copied();
            if actual_last_event_id != item.maybe_prev_stored_event_id {
                return Err(SaveEventsError::concurrent_modification());
            }
        }

        Self::save_event_rows_multi(connection_mut, &items).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRawEventStore for SqliteResourceRawEventStore {
    async fn total_events_stored_by_kind(&self, kind: &str) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let events_count = sqlx::query_scalar!(
            r#"
            SELECT
                COUNT(*) as "count!: i64"
            FROM resource_events
            WHERE resource_kind = $1
            "#,
            kind,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(events_count).unwrap())
    }

    fn get_all_events_by_kind(
        &self,
        kind: &str,
        opts: GetEventsOpts,
    ) -> EventStream<'_, ResourceRawEvent> {
        let kind = kind.to_string();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut rows = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id,
                    resource_id as "id: _",
                    resource_kind as "kind!",
                    event_time as "event_time: DateTime<Utc>",
                    event_type,
                    event_payload as "payload: serde_json::Value"
                FROM resource_events
                WHERE resource_kind = $1
                  AND ($2 IS NULL OR event_id > $2)
                  AND ($3 IS NULL OR event_id <= $3)
                ORDER BY event_id
                "#,
                kind,
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
                        id: ResourceID::new(row.id),
                    },
                    event_time: row.event_time,
                    event_type: row.event_type,
                    payload: row.payload,
                }));
            }
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
