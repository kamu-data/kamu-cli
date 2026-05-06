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
    ResourceRawEvent,
    ResourceRawEventProjection,
    ResourceRawEventQuery,
    ResourceRawEventStore,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceRawEventStore)]
pub struct PostgresRawResourceEventStore {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl PostgresRawResourceEventStore {
    async fn save_event_rows(
        connection_mut: &mut sqlx::PgConnection,
        query: &ResourceRawEventQuery,
        events: Vec<ResourceRawEvent>,
    ) -> Result<EventID, SaveEventsError> {
        assert!(!events.is_empty());

        // Prepare arrays for bulk insert

        let mut event_times = Vec::with_capacity(events.len());
        let mut event_types = Vec::with_capacity(events.len());
        let mut event_payloads = Vec::with_capacity(events.len());

        for event in events {
            event_times.push(event.event_time);
            event_types.push(event.event_type);
            event_payloads.push(event.payload);
        }

        let inserted: i64 = sqlx::query_scalar!(
            r#"
            WITH inserted AS (
                INSERT INTO resource_events (
                    resource_uid,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload
                )
                SELECT
                    $1,
                    $2,
                    e.event_time,
                    e.event_type,
                    e.event_payload
                FROM UNNEST(
                    $3::timestamptz[],
                    $4::text[],
                    $5::jsonb[]
                ) AS e(event_time, event_type, event_payload)
                RETURNING event_id
            )
            SELECT MAX(event_id) AS "event_id!" FROM inserted
            "#,
            *query.uid.as_ref(),
            &query.kind,
            &event_times,
            &event_types,
            &event_payloads,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()
        .map_err(SaveEventsError::Internal)?;

        Ok(EventID::new(inserted))
    }

    async fn save_event_rows_multi(
        connection_mut: &mut sqlx::PgConnection,
        items: Vec<SaveEventsItem<ResourceRawEventQuery, ResourceRawEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        let num_total_events = items.iter().map(|item| item.events.len()).sum();
        let num_items = items.len();

        // Prepare arrays for bulk insert

        let mut resource_uids = Vec::with_capacity(num_total_events);
        let mut resource_kinds = Vec::with_capacity(num_total_events);
        let mut event_times = Vec::with_capacity(num_total_events);
        let mut event_types = Vec::with_capacity(num_total_events);
        let mut event_payloads = Vec::with_capacity(num_total_events);
        let mut item_indexes = Vec::with_capacity(num_total_events);
        let mut event_indexes = Vec::with_capacity(num_total_events);

        for (item_index, item) in items.into_iter().enumerate() {
            let item_index = i64::try_from(item_index).unwrap();
            for (event_index, event) in item.events.into_iter().enumerate() {
                resource_uids.push(*item.query.uid.as_ref());
                resource_kinds.push(item.query.kind.clone());
                event_times.push(event.event_time);
                event_types.push(event.event_type);
                event_payloads.push(event.payload);
                item_indexes.push(item_index);
                event_indexes.push(i64::try_from(event_index).unwrap());
            }
        }

        // The bulk insert query

        #[derive(Debug, sqlx::FromRow)]
        struct InsertedEventItemRow {
            item_index: i64,
            event_id: i64,
        }

        let inserted_rows = sqlx::query_as!(
            InsertedEventItemRow,
            r#"
            WITH input AS (
                SELECT
                    ROW_NUMBER() OVER (ORDER BY item_index, event_index) AS row_num,
                    resource_uid,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload,
                    item_index
                FROM UNNEST(
                    $1::uuid[],
                    $2::text[],
                    $3::timestamptz[],
                    $4::text[],
                    $5::jsonb[],
                    $6::bigint[],
                    $7::bigint[]
                ) AS e(
                    resource_uid,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload,
                    item_index,
                    event_index
                )
            ),
            inserted AS (
                INSERT INTO resource_events (
                    resource_uid,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload
                )
                SELECT
                    resource_uid,
                    resource_kind,
                    event_time,
                    event_type,
                    event_payload
                FROM input
                ORDER BY row_num
                RETURNING event_id
            ),
            numbered_inserted AS (
                SELECT
                    event_id,
                    ROW_NUMBER() OVER (ORDER BY event_id) AS row_num
                FROM inserted
            )
            SELECT
                input.item_index as "item_index!",
                MAX(numbered_inserted.event_id) AS "event_id!"
            FROM input
            JOIN numbered_inserted USING (row_num)
            GROUP BY input.item_index
            ORDER BY input.item_index
            "#,
            &resource_uids,
            &resource_kinds,
            &event_times,
            &event_types,
            &event_payloads,
            &item_indexes,
            &event_indexes,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
        .map_err(SaveEventsError::Internal)?;

        let mut event_ids = vec![None; num_items];
        for row in inserted_rows {
            let item_index = usize::try_from(row.item_index).unwrap();
            event_ids[item_index] = Some(EventID::new(row.event_id));
        }

        Ok(event_ids
            .into_iter()
            .map(|event_id| event_id.expect("Every item has events"))
            .collect())
    }

    async fn get_last_event_id(
        connection_mut: &mut sqlx::PgConnection,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<EventID>, InternalError> {
        let query_uid: &uuid::Uuid = query.uid.as_ref();
        let last_event_id = sqlx::query_scalar!(
            r#"
            SELECT event_id
            FROM resource_events
            WHERE
                resource_uid = $1 AND resource_kind = $2
            ORDER BY event_id DESC
            LIMIT 1
            "#,
            query_uid,
            query.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()
        .map(|maybe_event_id| maybe_event_id.map(EventID::new))?;

        Ok(last_event_id)
    }

    async fn get_last_event_ids(
        connection_mut: &mut sqlx::PgConnection,
        queries: &[ResourceRawEventQuery],
    ) -> Result<HashMap<ResourceRawEventQuery, EventID>, InternalError> {
        if queries.is_empty() {
            return Ok(HashMap::new());
        }

        let uids: Vec<uuid::Uuid> = queries.iter().map(|q| *q.uid.as_ref()).collect();
        let kinds: Vec<String> = queries.iter().map(|q| q.kind.clone()).collect();

        #[derive(Debug, sqlx::FromRow)]
        struct LastEventRow {
            uid: uuid::Uuid,
            kind: String,
            event_id: i64,
        }

        let rows = sqlx::query_as!(
            LastEventRow,
            r#"
            SELECT
                resource_uid AS "uid: uuid::Uuid",
                resource_kind AS "kind!",
                MAX(event_id) AS "event_id!"
            FROM resource_events
            WHERE (resource_uid, resource_kind) IN (
                SELECT * FROM UNNEST($1::uuid[], $2::text[])
            )
            GROUP BY resource_uid, resource_kind
            "#,
            &uids,
            &kinds,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    ResourceRawEventQuery {
                        kind: row.kind,
                        uid: ResourceUID::new(row.uid),
                    },
                    EventID::new(row.event_id),
                )
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<ResourceRawEventProjection> for PostgresRawResourceEventStore {
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let events_count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!" FROM resource_events
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

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_uid as uid,
                    resource_kind as "kind!",
                    event_time,
                    event_type,
                    event_payload as "payload!"
                FROM resource_events
                WHERE ($1::bigint IS NULL OR event_id > $1)
                  AND ($2::bigint IS NULL OR event_id <= $2)
                ORDER BY event_id
                "#,
                opts.from.map(EventID::into_inner),
                opts.to.map(EventID::into_inner),
            )
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                yield Ok((event_id, ResourceRawEvent {
                    event_id,
                    query: ResourceRawEventQuery {
                        kind: row.kind,
                        uid: ResourceUID::new(row.uid),
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
        let key = query.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;
            let query_uid: &uuid::Uuid = key.uid.as_ref();

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_uid as uid,
                    resource_kind as "kind!",
                    event_time,
                    event_type,
                    event_payload as "payload!"
                FROM resource_events
                WHERE resource_uid = $1
                  AND resource_kind = $2
                  AND ($3::bigint IS NULL OR event_id > $3)
                  AND ($4::bigint IS NULL OR event_id <= $4)
                ORDER BY event_id
                "#,
                query_uid,
                key.kind,
                opts.from.map(EventID::into_inner),
                opts.to.map(EventID::into_inner),
            )
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                yield Ok((event_id, ResourceRawEvent {
                    event_id,
                    query: ResourceRawEventQuery {
                        kind: row.kind,
                        uid: ResourceUID::new(row.uid),
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

        let uids: Vec<uuid::Uuid> = queries.iter().map(|q| *q.uid.as_ref()).collect();
        let kinds: Vec<String> = queries.iter().map(|q| q.kind.clone()).collect();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_uid as "uid: uuid::Uuid",
                    resource_kind as "kind!",
                    event_time as "event_time: DateTime<Utc>",
                    event_type,
                    event_payload as "payload: serde_json::Value"
                FROM resource_events e
                WHERE (resource_uid, resource_kind) IN (
                    SELECT * FROM UNNEST($1::uuid[], $2::text[])
                )
                ORDER BY event_id
                "#,
                &uids,
                &kinds,
            )
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = rows.try_next().await? {
                let event_id = EventID::new(row.event_id);
                let query = ResourceRawEventQuery {
                    kind: row.kind,
                    uid: ResourceUID::new(row.uid),
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

        Self::save_event_rows(connection_mut, query, events).await
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

        Self::save_event_rows_multi(connection_mut, items).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRawEventStore for PostgresRawResourceEventStore {
    async fn total_events_stored_by_kind(&self, kind: &str) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let events_count = sqlx::query_scalar!(
            r#"
            SELECT
                COUNT(*) as "count!"
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

            let mut rows = sqlx::query!(
                r#"
                SELECT
                    event_id,
                    resource_uid as uid,
                    resource_kind as "kind!",
                    event_time,
                    event_type,
                    event_payload as "payload!"
                FROM resource_events
                WHERE resource_kind = $1
                  AND ($2::bigint IS NULL OR event_id > $2)
                  AND ($3::bigint IS NULL OR event_id <= $3)
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
                        uid: ResourceUID::new(row.uid),
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
