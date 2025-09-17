// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::postgres::PgRow;
use sqlx::{FromRow, Postgres, QueryBuilder};

use crate::helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SYSTEM_INITIATOR: &str = "<system>";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowEventStore)]
pub struct PostgresFlowEventStore {
    transaction: TransactionRefT<Postgres>,
}

impl PostgresFlowEventStore {
    fn prepare_initiator_filter(by_initiator: &InitiatorFilter) -> Vec<String> {
        match by_initiator {
            InitiatorFilter::System => vec![SYSTEM_INITIATOR.to_string()],
            InitiatorFilter::Account(a) => a.iter().map(ToString::to_string).collect(),
        }
    }

    async fn register_flow(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        e: &FlowEventInitiated,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let initiator = e
            .activation_cause
            .initiator_account_id()
            .map_or_else(|| SYSTEM_INITIATOR.to_string(), ToString::to_string);

        let flow_id: i64 = e.flow_id.try_into().unwrap();

        let scope_data_json = serde_json::to_value(&e.flow_binding.scope).int_err()?;

        sqlx::query!(
            r#"
            INSERT INTO flows (flow_id, flow_type, scope_data, initiator, flow_status, last_event_id)
                VALUES ($1, $2, $3, $4, 'waiting'::flow_status_type, NULL)
            "#,
            flow_id,
            e.flow_binding.flow_type,
            scope_data_json,
            initiator,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_flow_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        flow_id: FlowID,
        events: &[FlowEvent],
        maybe_prev_stored_event_id: Option<EventID>,
        last_event_id: EventID,
    ) -> Result<(), SaveEventsError> {
        let flow_id: i64 = flow_id.try_into().unwrap();
        let last_event_id: i64 = last_event_id.into();
        let maybe_prev_stored_event_id: Option<i64> = maybe_prev_stored_event_id.map(Into::into);

        // Determine if we have a status change between these events
        let mut maybe_latest_status = None;
        let mut maybe_scheduled_for_activation_at = None;
        let mut reset_scheduled_for_activation_at = false;

        for event in events {
            if let Some(new_status) = event.new_status() {
                maybe_latest_status = Some(new_status);
            }
            match event {
                FlowEvent::ScheduledForActivation(e) => {
                    maybe_scheduled_for_activation_at = Some(e.scheduled_for_activation_at);
                }
                FlowEvent::TaskFinished(e) => {
                    if let Some(next_activation_time) = e.next_attempt_at {
                        maybe_scheduled_for_activation_at = Some(next_activation_time);
                    }
                }
                FlowEvent::Aborted(_) | FlowEvent::TaskScheduled(_) => {
                    maybe_scheduled_for_activation_at = None;
                    reset_scheduled_for_activation_at = true;
                }
                _ => {
                    reset_scheduled_for_activation_at = false;
                }
            }
        }

        if maybe_scheduled_for_activation_at.is_none() && !reset_scheduled_for_activation_at {
            let connection_mut = tr.connection_mut().await?;
            maybe_scheduled_for_activation_at = sqlx::query!(
                r#"
                SELECT scheduled_for_activation_at as activation_time
                    FROM flows
                    WHERE flow_id = $1
                "#,
                flow_id
            )
            .map(|result| result.activation_time)
            .fetch_one(connection_mut)
            .await
            .int_err()?;
        }

        let connection_mut = tr.connection_mut().await?;
        let rows = sqlx::query!(
            r#"
            UPDATE flows
                SET flow_status = CASE WHEN $2::flow_status_type IS NOT NULL THEN $2::flow_status_type ELSE flow_status END,
                last_event_id = $3,
                scheduled_for_activation_at = $4
            WHERE flow_id = $1 AND (
                last_event_id IS NULL AND CAST($5 as BIGINT) IS NULL OR
                last_event_id IS NOT NULL AND CAST($5 as BIGINT) IS NOT NULL AND last_event_id = $5
            )
            RETURNING flow_id
            "#,
            flow_id,
            maybe_latest_status as Option<FlowStatus>,
            last_event_id,
            maybe_scheduled_for_activation_at,
            maybe_prev_stored_event_id,
        )
        .fetch_all(connection_mut)
        .await
        .map_err(|e| SaveEventsError::Internal(e.int_err()))?;

        // If a previously stored event id does not match the expected,
        // this means we've just detected a concurrent modification (version conflict)
        if rows.len() != 1 {
            return Err(SaveEventsError::concurrent_modification());
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        events: &[FlowEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(
            r#"
            INSERT INTO flow_events (flow_id, event_time, event_type, event_payload)
            "#,
        );

        query_builder.push_values(events, |mut b, event| {
            let event_flow_id: i64 = (event.flow_id()).try_into().unwrap();
            b.push_bind(event_flow_id);
            b.push_bind(event.event_time());
            b.push_bind(event.typename());
            b.push_bind(serde_json::to_value(event).unwrap());
        });

        query_builder.push("RETURNING event_id");

        let rows = query_builder
            .build_query_as::<ResultRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        let last_event_id = rows.last().unwrap().event_id;
        Ok(EventID::new(last_event_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for PostgresFlowEventStore {
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<FlowEvent> {
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
                FROM flow_events
                WHERE
                    (cast($1 as INT8) IS NULL OR event_id > $1) AND
                    (cast($2 as INT8) IS NULL OR event_id <= $2)
                ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowEvent>(event_row.event_payload)
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

    fn get_events(&self, flow_id: &FlowID, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        let flow_id: i64 = (*flow_id).try_into().unwrap();
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
                FROM flow_events
                WHERE flow_id = $1
                    AND (cast($2 as INT8) IS NULL OR event_id > $2)
                    AND (cast($3 as INT8) IS NULL OR event_id <= $3)
                ORDER BY event_id ASC
                "#,
                flow_id,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowEvent>(event_row.event_payload)
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

    fn get_events_multi(&self, queries: &[FlowID]) -> MultiEventStream<FlowID, FlowEvent> {
        let flow_ids: Vec<i64> = queries.iter().map(|id| (*id).try_into().unwrap()).collect();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT flow_id, event_id, event_payload
                FROM flow_events
                WHERE flow_id = ANY($1)
                ORDER BY event_id ASC
                "#,
                &flow_ids,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<FlowEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((FlowID::try_from(event_row.flow_id).unwrap(), // ids are always > 0
                    EventID::new(event_row.event_id),
                    event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((flow_id, event_id, event)) = query_stream.try_next().await? {
                yield Ok((flow_id, event_id, event));
            }
        })
    }

    async fn save_events(
        &self,
        flow_id: &FlowID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<FlowEvent>,
    ) -> Result<EventID, SaveEventsError> {
        // If there is nothing to save, exit quickly
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        // For the newly created flow, make sure it's registered before events
        let first_event = events.first().expect("Non empty event list expected");
        if let FlowEvent::Initiated(e) = first_event {
            assert_eq!(flow_id, &e.flow_id);

            // When creating a flow, there is no way something was already stored
            if maybe_prev_stored_event_id.is_some() {
                return Err(SaveEventsError::concurrent_modification());
            }

            // Make registration
            self.register_flow(&mut tr, e).await?;
        }

        // Save events one by one
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        // Update denormalized flow record: latest status and stored event
        self.update_flow_from_events(
            &mut tr,
            *flow_id,
            &events,
            maybe_prev_stored_event_id,
            last_event_id,
        )
        .await?;

        Ok(last_event_id)
    }

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count
                FROM flow_events
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
impl FlowEventStore for PostgresFlowEventStore {
    async fn new_flow_id(&self) -> Result<FlowID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT nextval('flow_id_seq') AS new_flow_id
            "#
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let flow_id = result.new_flow_id.unwrap();
        Ok(FlowID::try_from(flow_id).unwrap())
    }

    async fn try_get_pending_flow(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let flow_type = flow_binding.flow_type.as_str();
        let scope_data_json = serde_json::to_value(&flow_binding.scope).int_err()?;

        let maybe_flow_id = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    flow_type = $1 AND
                    scope_data = $2 AND
                    flow_status != 'finished'::flow_status_type
                ORDER BY flow_id DESC
                LIMIT 1
            "#,
            flow_type,
            scope_data_json,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?
        .map(|r| r.flow_id);

        Ok(maybe_flow_id.map(|id| FlowID::try_from(id).unwrap()))
    }

    async fn try_get_all_scope_pending_flows(
        &self,
        flow_scope: &FlowScope,
    ) -> Result<Vec<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let flow_scope = serde_json::to_value(flow_scope).int_err()?;

        let flow_ids = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    scope_data = $1 AND
                    flow_status != 'finished'::flow_status_type
                ORDER BY flow_id DESC
            "#,
            flow_scope,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    /// Returns nearest time when one or more flows are scheduled for activation
    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let maybe_activation_time = sqlx::query!(
            r#"
            SELECT f.scheduled_for_activation_at as activation_time
                FROM flows f
                WHERE
                    f.scheduled_for_activation_at IS NOT NULL AND
                    (f.flow_status = 'waiting'::flow_status_type OR f.flow_status = 'retrying'::flow_status_type)
                ORDER BY f.scheduled_for_activation_at ASC
                LIMIT 1
            "#,
        )
        .map(|result| {
            result
                .activation_time
                .expect("NULL values filtered by query")
        })
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_activation_time)
    }

    /// Returns flows scheduled for activation at the given time
    async fn get_flows_scheduled_for_activation_at(
        &self,
        scheduled_for_activation_at: DateTime<Utc>,
    ) -> Result<Vec<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let flow_ids = sqlx::query!(
            r#"
            SELECT f.flow_id as flow_id
                FROM flows f
                WHERE
                    f.scheduled_for_activation_at = $1 AND
                    (f.flow_status = 'waiting'::flow_status_type OR f.flow_status = 'retrying'::flow_status_type)
                ORDER BY f.flow_id ASC
            "#,
            scheduled_for_activation_at,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    fn get_all_flow_ids_matching_scope_query(
        &self,
        flow_scope_query: FlowScopeQuery,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let by_flow_types = filters.by_flow_types.clone();
        let by_flow_status = filters.by_flow_status;

        let pagination_limit = i64::try_from(pagination.limit).unwrap();
        let pagination_offset = i64::try_from(pagination.offset).unwrap();

        let (scope_conditions, _) =
            generate_scope_query_condition_clauses(&flow_scope_query, 6 /* 5 params + 1 */);

        let scope_values = form_scope_query_condition_values(flow_scope_query);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    ({scope_conditions})
                    AND (cast($1 as TEXT[]) IS NULL OR flow_type = ANY($1))
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
                ORDER BY flow_status, last_event_id DESC
                LIMIT $4 OFFSET $5
                "#,
            );

            let mut query = sqlx::query(&query_str)
                .bind(by_flow_types as Option<Vec<String>>)
                .bind(by_flow_status as Option<FlowStatus>)
                .bind(maybe_initiators as Option<Vec<String>>)
                .bind(pagination_limit)
                .bind(pagination_offset);

            for values in scope_values {
                query = query.bind(values);
            }

            use sqlx::Row;
            let mut query_stream = query
                .try_map(|event_row: PgRow| {
                    let flow_id: i64 = event_row.get(0);
                    Ok(FlowID::new(u64::try_from(flow_id).unwrap()))
                })
                .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_flows_matching_scope_query(
        &self,
        flow_scope_query: &FlowScopeQuery,
        filters: &FlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let (scope_conditions, _) =
            generate_scope_query_condition_clauses(flow_scope_query, 4 /* 3 params + 1 */);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                ({scope_conditions})
                AND (cast($1 as TEXT[]) IS NULL OR flow_type = ANY($1))
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
            "#,
        );

        let mut query = sqlx::query(&query_str)
            .bind(filters.by_flow_types.clone())
            .bind(filters.by_flow_status as Option<FlowStatus>)
            .bind(maybe_initiators as Option<Vec<String>>);

        for (_, values) in &flow_scope_query.attributes {
            query = query.bind(values);
        }

        let query_result = query.fetch_one(connection_mut).await.int_err()?;

        use sqlx::Row;
        let flows_count: i64 = query_result.get(0);
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn list_scoped_flow_initiators(&self, flow_scope_query: FlowScopeQuery) -> InitiatorIDStream {
        let (scope_conditions, _) =
            generate_scope_query_condition_clauses(&flow_scope_query, 1 /* no params + 1 */);

        let scope_values = form_scope_query_condition_values(flow_scope_query);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT DISTINCT(initiator) FROM flows
                WHERE
                    ({scope_conditions})
                    AND initiator != '{SYSTEM_INITIATOR}'
                "#,
            );

            let mut query = sqlx::query(&query_str);
            for values in scope_values {
                query = query.bind(values);
            }

            let mut query_stream = query.fetch(connection_mut);

            use sqlx::Row;
            while let Some(event_row) = query_stream.try_next().await.int_err()? {
                let initiator_id_str: &str = event_row.get("initiator");
                let initiator_id = odf::AccountID::from_did_str(initiator_id_str).unwrap();
                yield Ok(initiator_id);
            }
        })
    }

    fn get_all_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_> {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_by_flow_types = filters.by_flow_types.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    (cast($1 as TEXT[]) IS NULL OR flow_type = ANY($1))
                    AND (cast($2 as flow_status_type) IS NULL or flow_status = $2)
                    AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_by_flow_types.as_deref(),
                maybe_by_flow_status as Option<FlowStatus>,
                maybe_initiators.as_deref(),
                i64::try_from(pagination.limit).unwrap(),
                i64::try_from(pagination.offset).unwrap(),
            ).try_map(|event_row| {
                let flow_id = event_row.flow_id;
                Ok(FlowID::try_from(flow_id).unwrap())
            })
            .fetch(connection_mut);

            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(flow_id);
            }
        })
    }

    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_by_flow_types = filters.by_flow_types.as_deref();

        let query_result = sqlx::query!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE
                    (cast($1 as TEXT[]) IS NULL OR flow_type = ANY($1))
                    AND (cast($2 as flow_status_type) IS NULL or flow_status = $2)
                    AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
            "#,
            maybe_by_flow_types,
            maybe_by_flow_status as Option<FlowStatus>,
            maybe_initiators.as_deref(),
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let flows_count = query_result.flows_count.unwrap_or_default();
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream {
        Box::pin(async_stream::try_stream! {
            const CHUNK_SIZE: usize = 256;
            for chunk in flow_ids.chunks(CHUNK_SIZE) {
                let flows = Flow::load_multi(
                    chunk,
                    self
                ).await.int_err()?;
                for flow in flows {
                    yield flow.int_err()?.into();
                }
            }
        })
    }

    async fn filter_flow_scopes_having_flows(
        &self,
        flow_scopes: &[FlowScope],
    ) -> Result<Vec<FlowScope>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let scope_json_parts = flow_scopes
            .iter()
            .map(|scope| serde_json::to_value(scope).unwrap())
            .collect::<Vec<_>>();

        let filtered_flow_scopes = sqlx::query!(
            r#"
            SELECT DISTINCT scope_data
            FROM flows
            WHERE
                scope_data = ANY($1)
            "#,
            &scope_json_parts,
        )
        .map(|flow_row| serde_json::from_value::<FlowScope>(flow_row.scope_data).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(filtered_flow_scopes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
