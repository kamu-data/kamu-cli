// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::TransactionRefT;
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::{FromRow, Postgres, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowTriggerEventStore)]
pub struct PostgresFlowTriggerEventStore {
    transaction: TransactionRefT<Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowTriggerState> for PostgresFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, FlowTriggerEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM flow_trigger_events
                WHERE
                    (cast($1 as BIGINT) IS NULL or event_id > $1) AND
                    (cast($2 as BIGINT) IS NULL or event_id <= $2)
                ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowTriggerEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield Ok((event_id, event));
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    fn get_events(
        &self,
        flow_binding: &FlowBinding,
        opts: GetEventsOpts,
    ) -> EventStream<'_, FlowTriggerEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        let flow_type = flow_binding.flow_type.clone();
        let scope_json = serde_json::to_value(&flow_binding.scope).unwrap();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload
                FROM flow_trigger_events
                WHERE flow_type = $1
                    AND scope_data = $2
                    AND (cast($3 as BIGINT) IS NULL or event_id > $3)
                    AND (cast($4 as BIGINT) IS NULL or event_id <= $4)
                ORDER BY event_id
                "#,
                flow_type,
                scope_json,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowTriggerEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield Ok((event_id, event));
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(num_queries = queries.len()))]
    fn get_events_multi(
        &self,
        queries: &[FlowBinding],
    ) -> MultiEventStream<'_, FlowBinding, FlowTriggerEvent> {
        let queries = queries.to_vec();
        let flow_types: Vec<String> = queries.iter().map(|b| b.flow_type.clone()).collect();
        let scopes_json: Vec<serde_json::Value> = queries
            .iter()
            .map(|b| serde_json::to_value(&b.scope).unwrap())
            .collect();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT q.idx AS "idx!", e.event_id, e.event_payload
                FROM flow_trigger_events e
                    JOIN UNNEST($1::text[], $2::jsonb[]) WITH ORDINALITY AS q(flow_type, scope_data, idx)
                        ON e.flow_type = q.flow_type AND e.scope_data = q.scope_data
                ORDER BY e.event_id
                "#,
                &flow_types,
                &scopes_json,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowTriggerEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((event_row.idx, EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((idx, event_id, event)) = query_stream.try_next().await? {
                // Ordinality is 1-based
                let query_index = usize::try_from(idx - 1).unwrap();
                yield Ok((queries[query_index].clone(), event_id, event));
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_binding))]
    async fn save_events(
        &self,
        flow_binding: &FlowBinding,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowTriggerEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Rejects an expected event that is not the binding's last one. Two writers
        // based on the same event both pass it and collide on the unique index over
        // `prev_event_id` instead
        let last_stored_event_id = sqlx::query_scalar!(
            r#"
            SELECT MAX(event_id) AS "last_event_id?: i64"
                FROM flow_trigger_events
                WHERE flow_type = $1 AND scope_data = $2
            "#,
            flow_binding.flow_type.as_str(),
            scope_data_json,
        )
        .fetch_one(&mut *connection_mut)
        .await
        .int_err()?;

        if last_stored_event_id != maybe_prev_stored_event_id.map(EventID::into_inner) {
            return Err(SaveEventsError::concurrent_modification());
        }

        let mut query_builder = QueryBuilder::<Postgres>::new(
            r#"
            INSERT INTO flow_trigger_events (flow_type, scope_data, event_type, event_time, event_payload, prev_event_id)
            "#,
        );

        let batch_prev_event_id = maybe_prev_stored_event_id.map_or(0, EventID::into_inner);

        query_builder.push_values(events.into_iter().enumerate(), |mut b, (i, event)| {
            b.push_bind(flow_binding.flow_type.as_str());
            b.push_bind(&scope_data_json);
            b.push_bind(event.typename());
            b.push_bind(event.event_time());
            b.push_bind(serde_json::to_value(event).unwrap());
            b.push_bind((i == 0).then_some(batch_prev_event_id));
        });

        query_builder.push("RETURNING event_id");

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let rows = match query_builder
            .build_query_as::<ResultRow>()
            .fetch_all(connection_mut)
            .await
        {
            Ok(rows) => rows,
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                return Err(SaveEventsError::concurrent_modification());
            }
            Err(e) => return Err(SaveEventsError::Internal(e.int_err())),
        };
        let last_event_id = rows.last().unwrap().event_id;

        Ok(EventID::new(last_event_id))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(num_items = items.len()))]
    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<FlowBinding, FlowTriggerEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        validate_multi_save_items(&items)?;

        let item_flow_types: Vec<String> = items
            .iter()
            .map(|item| item.query.flow_type.clone())
            .collect();
        let item_scopes_json: Vec<serde_json::Value> = items
            .iter()
            .map(|item| serde_json::to_value(&item.query.scope))
            .collect::<Result<_, _>>()
            .int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Rejects an expected event that is not the binding's last one. Two writers
        // based on the same event both pass it and collide on the unique index over
        // `prev_event_id` instead
        let last_stored_event_ids = sqlx::query!(
            r#"
            SELECT q.idx AS "idx!", MAX(e.event_id) AS "last_event_id?: i64"
                FROM UNNEST($1::text[], $2::jsonb[]) WITH ORDINALITY AS q(flow_type, scope_data, idx)
                    LEFT JOIN flow_trigger_events e
                        ON e.flow_type = q.flow_type AND e.scope_data = q.scope_data
                GROUP BY q.idx
            "#,
            &item_flow_types,
            &item_scopes_json,
        )
        .fetch_all(&mut *connection_mut)
        .await
        .int_err()?;

        for row in last_stored_event_ids {
            let item = &items[usize::try_from(row.idx - 1).unwrap()];
            if row.last_event_id != item.maybe_prev_stored_event_id.map(EventID::into_inner) {
                return Err(SaveEventsError::concurrent_modification());
            }
        }

        let num_total_events = items.iter().map(|item| item.events.len()).sum();
        let mut flow_types = Vec::with_capacity(num_total_events);
        let mut scopes_json = Vec::with_capacity(num_total_events);
        let mut event_types = Vec::with_capacity(num_total_events);
        let mut event_times = Vec::with_capacity(num_total_events);
        let mut event_payloads = Vec::with_capacity(num_total_events);
        let mut prev_event_ids = Vec::with_capacity(num_total_events);
        let mut item_event_counts = Vec::with_capacity(items.len());

        for (item, (flow_type, scope_json)) in items
            .into_iter()
            .zip(item_flow_types.into_iter().zip(item_scopes_json))
        {
            let batch_prev_event_id = item
                .maybe_prev_stored_event_id
                .map_or(0, EventID::into_inner);
            item_event_counts.push(item.events.len());

            for (i, event) in item.events.into_iter().enumerate() {
                flow_types.push(flow_type.clone());
                scopes_json.push(scope_json.clone());
                event_types.push(event.typename().to_string());
                event_times.push(event.event_time());
                event_payloads.push(serde_json::to_value(event).int_err()?);
                prev_event_ids.push((i == 0).then_some(batch_prev_event_id));
            }
        }

        let insert_result = sqlx::query_scalar!(
            r#"
            INSERT INTO flow_trigger_events (flow_type, scope_data, event_type, event_time, event_payload, prev_event_id)
            SELECT flow_type, scope_data, event_type, event_time, event_payload, prev_event_id
                FROM UNNEST($1::text[], $2::jsonb[], $3::text[], $4::timestamptz[], $5::jsonb[], $6::bigint[])
                    WITH ORDINALITY AS e(flow_type, scope_data, event_type, event_time, event_payload, prev_event_id, row_num)
                ORDER BY row_num
            RETURNING event_id
            "#,
            &flow_types,
            &scopes_json,
            &event_types,
            &event_times,
            &event_payloads,
            &prev_event_ids as _,
        )
        .fetch_all(connection_mut)
        .await;

        let inserted_event_ids = match insert_result {
            Ok(event_ids) => event_ids,
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                return Err(SaveEventsError::concurrent_modification());
            }
            Err(e) => return Err(SaveEventsError::Internal(e.int_err())),
        };

        Ok(last_event_ids_per_item(
            inserted_event_ids,
            item_event_counts,
        )?)
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Without type cast, PostgreSQL, for some unknown reason, returned type
        // `bignumeric`, which is not suitable for us
        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_trigger_events
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.events_count.unwrap()).int_err()?;

        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTriggerEventStore for PostgresFlowTriggerEventStore {
    #[tracing::instrument(level = "debug", skip_all)]
    fn stream_all_active_flow_bindings(&self) -> FlowBindingStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let mut query_stream = sqlx::query!(
                r#"
                WITH latest_events AS (
                    SELECT DISTINCT ON (flow_type, scope_data)
                        flow_type,
                        scope_data,
                        event_type,
                        event_payload
                    FROM flow_trigger_events
                    ORDER BY flow_type, scope_data, event_id DESC
                )
                SELECT flow_type, scope_data
                FROM latest_events
                WHERE (
                    (event_type = 'FlowTriggerEventCreated' AND (event_payload #>> '{Created,paused}') = 'false')
                    OR
                    (event_type = 'FlowTriggerEventModified' AND (event_payload #>> '{Modified,paused}') = 'false')
                )
                "#,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                let flow_binding = FlowBinding {
                    flow_type: row.flow_type,
                    scope: serde_json::from_value(row.scope_data).int_err()?,
                };
                yield Ok(flow_binding);
            }
        })
    }

    #[tracing::instrument(level = "debug", skip_all, fields(?flow_scopes))]
    async fn all_trigger_bindings_for_scopes(
        &self,
        flow_scopes: &[FlowScope],
    ) -> Result<Vec<FlowBinding>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let scopes_json = flow_scopes
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        let flow_bindings = sqlx::query!(
            r#"
            SELECT DISTINCT flow_type, scope_data
                FROM flow_trigger_events
                WHERE scope_data = ANY($1)
                    AND event_type = 'FlowTriggerEventCreated'
            "#,
            &scopes_json,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        flow_bindings
            .into_iter()
            .map(|row| {
                let scope: FlowScope = serde_json::from_value(row.scope_data).int_err()?;
                Ok(FlowBinding {
                    flow_type: row.flow_type,
                    scope,
                })
            })
            .collect()
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn has_active_triggers_for_scopes(
        &self,
        scopes: &[FlowScope],
    ) -> Result<bool, InternalError> {
        if scopes.is_empty() {
            return Ok(false);
        }

        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;

        let scopes_json = scopes
            .iter()
            .map(serde_json::to_value)
            .collect::<Result<Vec<_>, _>>()
            .int_err()?;

        let has_active_triggers = sqlx::query_scalar!(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM (
                    SELECT DISTINCT ON (flow_type, scope_data)
                        scope_data,
                        event_type,
                        event_payload
                    FROM flow_trigger_events
                    WHERE
                        scope_data = ANY($1)
                    ORDER BY flow_type, scope_data, event_id DESC
                ) AS latest_events
                WHERE (
                    (event_type = 'FlowTriggerEventCreated' AND (event_payload#>>'{Created,paused}') = 'false') OR
                    (event_type = 'FlowTriggerEventModified' AND (event_payload#>>'{Modified,paused}') = 'false')
                )
            )
            "#,
            &scopes_json,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(has_active_triggers.unwrap_or(false))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
