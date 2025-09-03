// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRefT, sqlite_generate_placeholders_list};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use sqlx::{FromRow, QueryBuilder, Sqlite};

use crate::helpers::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SYSTEM_INITIATOR: &str = "<system>";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn FlowEventStore)]
pub struct SqliteFlowEventStore {
    transaction: TransactionRefT<Sqlite>,
}

impl SqliteFlowEventStore {
    fn prepare_initiator_filter(by_initiator: &InitiatorFilter) -> Vec<String> {
        match by_initiator {
            InitiatorFilter::System => vec![SYSTEM_INITIATOR.to_string()],
            InitiatorFilter::Account(a) => a.iter().map(ToString::to_string).collect(),
        }
    }

    async fn register_flow(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        e: &FlowEventInitiated,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let initiator = e
            .activation_cause
            .initiator_account_id()
            .map_or_else(|| SYSTEM_INITIATOR.to_string(), ToString::to_string);

        let flow_id: i64 = e.flow_id.try_into().unwrap();

        let scope_json = serde_json::to_value(&e.flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;

        sqlx::query!(
            r#"
            INSERT INTO flows (flow_id, flow_type, scope_data, initiator, flow_status, last_event_id)
                VALUES ($1, $2, $3, $4, 'waiting', NULL)
            "#,
            flow_id,
            e.flow_binding.flow_type,
            scope_json_str,
            initiator,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_flow_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
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

            if maybe_scheduled_for_activation_at.is_none() && !reset_scheduled_for_activation_at {
                #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
                #[allow(dead_code)]
                pub struct ActivationRow {
                    pub activation_time: Option<DateTime<Utc>>,
                }

                let connection_mut = tr.connection_mut().await?;
                maybe_scheduled_for_activation_at = sqlx::query_as!(
                    ActivationRow,
                    r#"
                    SELECT scheduled_for_activation_at as "activation_time: _"
                        FROM flows WHERE flow_id = $1
                    "#,
                    flow_id
                )
                .map(|result| result.activation_time)
                .fetch_one(connection_mut)
                .await
                .int_err()?;
            }
        }

        let connection_mut = tr.connection_mut().await?;
        let rows = sqlx::query!(
            r#"
            UPDATE flows
                SET flow_status = CASE WHEN $2 IS NOT NULL THEN $2 ELSE flow_status END,
                last_event_id = $3,
                scheduled_for_activation_at = $4
            WHERE flow_id = $1 AND (
                last_event_id IS NULL AND CAST($5 as INT8) IS NULL OR
                last_event_id IS NOT NULL AND CAST($5 as INT8) IS NOT NULL AND last_event_id = $5
            )
            RETURNING flow_id
            "#,
            flow_id,
            maybe_latest_status,
            last_event_id,
            maybe_scheduled_for_activation_at,
            maybe_prev_stored_event_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        // If a previously stored event id does not match the expected,
        // this means we've just detected a concurrent modification (version conflict)
        if rows.len() != 1 {
            return Err(SaveEventsError::concurrent_modification());
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        events: &[FlowEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = QueryBuilder::<sqlx::Sqlite>::new(
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
impl EventStore<FlowState> for SqliteFlowEventStore {
    fn get_events(&self, flow_id: &FlowID, opts: GetEventsOpts) -> EventStream<FlowEvent> {
        let flow_id: i64 = (*flow_id).try_into().unwrap();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            #[allow(dead_code)]
            pub struct EventModel {
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let mut query_stream = sqlx::query_as!(
                EventModel,
                r#"
                SELECT event_id, event_payload as "event_payload: _"
                FROM flow_events
                WHERE flow_id = $1
                    AND (cast($2 as INT8) IS NULL OR event_id > $2)
                    AND (cast($3 as INT8) IS NULL OR event_id <= $3)
                ORDER BY event_id
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

    fn get_events_multi(&self, queries: Vec<FlowID>) -> MultiEventStream<FlowID, FlowEvent> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;


            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub flow_id: i64,
                pub event_id: i64,
                pub event_payload: serde_json::Value,
            }

            let query_str = format!(
                r#"
                SELECT
                    flow_id,
                    event_id,
                    event_payload
                FROM flow_events
                    WHERE flow_id IN ({})
                    ORDER BY event_id
                "#,
                sqlite_generate_placeholders_list(
                    queries.len(),
                    NonZeroUsize::new(1).unwrap()
                )
            );

            let mut query = sqlx::query_as::<_, EventRow>(&query_str);
            for flow_id in queries {
                let flow_id: i64 = flow_id.try_into().unwrap();
                query = query.bind(flow_id);
            }

            let mut query_stream = query
                .fetch(connection_mut)
                .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(row) = query_stream.try_next().await? {
                let flow_id = FlowID::new(u64::try_from(row.flow_id).unwrap());
                let event_id = EventID::new(row.event_id);
                let event = serde_json::from_value::<FlowEvent>(row.event_payload)
                    .map_err(|e| GetEventsError::Internal(e.int_err()))?;

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

        let count = usize::try_from(result.events_count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
#[allow(dead_code)]
pub struct NewFlow {
    pub flow_id: i64,
}

#[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
#[allow(dead_code)]
pub struct RunStatsRow {
    pub last_event_time: DateTime<Utc>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowEventStore for SqliteFlowEventStore {
    async fn new_flow_id(&self) -> Result<FlowID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let created_time = Utc::now();

        let result = sqlx::query_as!(
            NewFlow,
            r#"
            INSERT INTO flow_ids(created_time) VALUES($1) RETURNING flow_id as "flow_id: _"
            "#,
            created_time
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(FlowID::try_from(result.flow_id).unwrap())
    }

    async fn try_get_pending_flow(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<Option<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let flow_type = flow_binding.flow_type.as_str();

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;

        let maybe_flow_id = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    flow_type = $1 AND
                    scope_data = $2 AND
                    flow_status != 'finished'
                ORDER BY flow_id DESC
                LIMIT 1
            "#,
            flow_type,
            scope_json_str,
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

        let scope_json = serde_json::to_value(flow_scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;

        let flow_ids = sqlx::query!(
            r#"
            SELECT flow_id FROM flows
                WHERE
                    scope_data = $1 AND
                    flow_status != 'finished'
                ORDER BY flow_id DESC
            "#,
            scope_json_str,
        )
        .map(|row| FlowID::try_from(row.flow_id).unwrap())
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(flow_ids)
    }

    async fn get_flow_run_stats(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let flow_type = flow_binding.flow_type.as_str();

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let row = sqlx::query!(
            r#"
            WITH binding AS (
                SELECT ? AS flow_type, ? AS scope_data
            )
            SELECT
                /* latest finished attempt */
                (
                    SELECT e.event_time
                    FROM flow_events e, binding b
                    WHERE e.event_type = 'FlowEventTaskFinished'
                    AND EXISTS (
                        SELECT 1
                        FROM flows f
                        WHERE f.flow_id = e.flow_id
                        AND f.flow_type = b.flow_type
                        AND f.scope_data = b.scope_data
                    )
                    ORDER BY e.event_id DESC
                    LIMIT 1
                ) AS "last_attempt_time: Option<DateTime<Utc>>",

                /* latest success */
                (
                    SELECT e.event_time
                    FROM flow_events e, binding b
                    WHERE e.event_type = 'FlowEventTaskFinished'
                    AND json_extract(e.event_payload, '$.TaskFinished.task_outcome.Success') IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM flows f
                        WHERE f.flow_id = e.flow_id
                        AND f.flow_type = b.flow_type
                        AND f.scope_data = b.scope_data
                    )
                    ORDER BY e.event_id DESC
                    LIMIT 1
                ) AS "last_success_time: DateTime<Utc>",

                /* latest failure */
                (
                    SELECT e.event_time
                    FROM flow_events e, binding b
                    WHERE e.event_type = 'FlowEventTaskFinished'
                    AND json_extract(e.event_payload, '$.TaskFinished.task_outcome.Failed') IS NOT NULL
                    AND EXISTS (
                        SELECT 1
                        FROM flows f
                        WHERE f.flow_id = e.flow_id
                        AND f.flow_type = b.flow_type
                        AND f.scope_data = b.scope_data
                    )
                    ORDER BY e.event_id DESC
                    LIMIT 1
                ) AS "last_failure_time: DateTime<Utc>"
            "#,
            flow_type,
            scope_json_str,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(match row {
            Some(r) => FlowRunStats {
                last_attempt_time: r.last_attempt_time,
                last_success_time: r.last_success_time,
                last_failure_time: r.last_failure_time,
            },
            None => FlowRunStats::default(),
        })
    }

    async fn get_current_consecutive_flow_failures_count(
        &self,
        flow_binding: &FlowBinding,
    ) -> Result<u32, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let flow_type = flow_binding.flow_type.as_str();

        let scope_json = serde_json::to_value(&flow_binding.scope).int_err()?;
        let scope_json_str = canonical_json::to_string(&scope_json).int_err()?;

        let consecutive_failures_count = sqlx::query!(
            r#"
            WITH finished AS (
                SELECT
                    e.event_id,
                    json_extract(e.event_payload, '$.TaskFinished.task_outcome.Success') IS NOT NULL AS is_success,
                    json_extract(e.event_payload, '$.TaskFinished.task_outcome.Failed')  IS NOT NULL AS is_failed
                FROM flow_events e
                JOIN flows f ON f.flow_id = e.flow_id
                WHERE
                    e.event_type = 'FlowEventTaskFinished'
                    AND f.flow_type = $1
                    AND f.scope_data = $2
            ),
            last_success AS (
                SELECT event_id
                FROM finished
                WHERE is_success
                ORDER BY event_id DESC
                LIMIT 1
            )
            SELECT
                COUNT(*) AS consecutive_failures
            FROM finished
            WHERE
                is_failed AND event_id > IFNULL((SELECT event_id FROM last_success), 0);
            "#,
            flow_type,
            scope_json_str,
        )
        .map(|event_row| event_row.consecutive_failures)
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let failures_count: u32 = consecutive_failures_count.try_into().int_err()?;

        Ok(failures_count)
    }

    async fn consecutive_flow_failures_by_binding(
        &self,
        flow_bindings: Vec<FlowBinding>,
    ) -> Result<Vec<(FlowBinding, u32)>, InternalError> {
        if flow_bindings.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(flow_bindings.len());

        for binding in flow_bindings {
            let failures_count = self
                .get_current_consecutive_flow_failures_count(&binding)
                .await?;
            results.push((binding, failures_count));
        }

        Ok(results)
    }

    async fn nearest_flow_activation_moment(&self) -> Result<Option<DateTime<Utc>>, InternalError> {
        let mut tr = self.transaction.lock().await;

        #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
        #[allow(dead_code)]
        pub struct ActivationRow {
            pub activation_time: Option<DateTime<Utc>>,
        }

        let connection_mut = tr.connection_mut().await?;
        let maybe_activation_time = sqlx::query_as!(
            ActivationRow,
            r#"
            SELECT f.scheduled_for_activation_at as "activation_time: _"
                FROM flows f
                WHERE
                    f.scheduled_for_activation_at IS NOT NULL AND
                    (f.flow_status = 'waiting' OR f.flow_status = 'retrying')
                ORDER BY f.scheduled_for_activation_at
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
                    (f.flow_status = 'waiting' OR f.flow_status = 'retrying')
                ORDER BY f.flow_id
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

        let maybe_by_flow_type = filters.by_flow_type.clone();
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let (scope_clauses, next_parameter_index) = generate_scope_query_condition_clauses(&flow_scope_query, 6 /* 5 params + 1 */);
            let scope_values = form_scope_query_condition_values(flow_scope_query);

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    ({scope_clauses})
                    AND ($1::text IS NULL OR flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator IN ({}))
                ORDER BY flow_status DESC, last_event_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(next_parameter_index).unwrap()
                    ))
                    .unwrap_or_default(),
            );

            let mut query = sqlx::query_scalar::<_, i64>(&query_str)
                .bind(maybe_by_flow_type)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            for value in scope_values {
                query = query.bind(value);
            }

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.fetch(connection_mut);
            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(FlowID::new(u64::try_from(flow_id).unwrap()));
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

        let maybe_filters_by_flow_type = filters.by_flow_type.as_deref();
        let maybe_filters_by_flow_status = filters.by_flow_status;

        let (scope_clauses, next_parameter_index) =
            generate_scope_query_condition_clauses(flow_scope_query, 4 /* 3 params + 1 */);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                ({scope_clauses})
                AND ($1::text IS NULL OR flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| {
                    sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(next_parameter_index).unwrap(),
                    )
                })
                .unwrap_or_default()
        );

        let mut query = sqlx::query_scalar::<_, i64>(&query_str)
            .bind(maybe_filters_by_flow_type)
            .bind(maybe_filters_by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        for (_, values) in &flow_scope_query.attributes {
            for value in values {
                query = query.bind(value);
            }
        }

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let flows_count = query.fetch_one(connection_mut).await.int_err()?;
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn list_scoped_flow_initiators(&self, flow_scope_query: FlowScopeQuery) -> InitiatorIDStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let (scope_clauses, _) =
                generate_scope_query_condition_clauses(&flow_scope_query, 1 /* no params + 1 */);

            let scope_values = form_scope_query_condition_values(flow_scope_query);

            let query_str = format!(
                r#"
                SELECT DISTINCT(initiator) FROM flows
                WHERE
                    ({scope_clauses})
                    AND initiator != '{SYSTEM_INITIATOR}'
                "#,
            );

            let mut query = sqlx::query_scalar::<_, String>(&query_str);
            for value in scope_values {
                query = query.bind(value);
            }

            let mut query_stream = query.fetch(connection_mut);
            while let Some(initiator_id_str) = query_stream.try_next().await.int_err()? {
                yield Ok(odf::AccountID::from_did_str(&initiator_id_str).unwrap());
            }
        })
    }

    fn get_all_flow_ids(
        &self,
        filters: &FlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_> {
        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type.clone();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr.connection_mut().await?;

            let query_str = format!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    ($1::text IS NULL OR flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                    AND ($3 = 0 OR initiator IN ({}))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_initiators
                    .as_ref()
                    .map(|initiators| {
                        sqlite_generate_placeholders_list(initiators.len(), NonZeroUsize::new(5).unwrap())
                    })
                    .unwrap_or_default()
            );

            let mut query = sqlx::query_scalar::<_, i64>(&query_str)
                .bind(maybe_by_flow_type)
                .bind(maybe_by_flow_status)
                .bind(i32::from(maybe_initiators.is_some()))
                .bind(i64::try_from(pagination.limit).unwrap())
                .bind(i64::try_from(pagination.offset).unwrap());

            if let Some(initiators) = maybe_initiators {
                for initiator in initiators {
                    query = query.bind(initiator);
                }
            }

            let mut query_stream = query.fetch(connection_mut);
            while let Some(flow_id) = query_stream.try_next().await.int_err()? {
                yield Ok(FlowID::new(u64::try_from(flow_id).unwrap()));
            }
        })
    }

    async fn get_count_all_flows(&self, filters: &FlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_by_flow_status = filters.by_flow_status;

        let maybe_by_flow_type = filters.by_flow_type.as_deref();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_str = format!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
            WHERE
                ($1::text IS NULL OR flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL OR flow_status = $2)
                AND ($3 = 0 OR initiator IN ({}))
            "#,
            maybe_initiators
                .as_ref()
                .map(|initiators| {
                    sqlite_generate_placeholders_list(
                        initiators.len(),
                        NonZeroUsize::new(3).unwrap(),
                    )
                })
                .unwrap_or_default()
        );

        let mut query = sqlx::query_scalar::<_, i64>(&query_str)
            .bind(maybe_by_flow_type)
            .bind(maybe_by_flow_status)
            .bind(i32::from(maybe_initiators.is_some()));

        if let Some(initiators) = maybe_initiators {
            for initiator in initiators {
                query = query.bind(initiator);
            }
        }

        let flows_count = query.fetch_one(connection_mut).await.int_err()?;
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_stream(&self, flow_ids: Vec<FlowID>) -> FlowStateStream {
        Box::pin(async_stream::try_stream! {
            const CHUNK_SIZE: usize = 256;
            for chunk in flow_ids.chunks(CHUNK_SIZE) {
                let flows = Flow::load_multi(
                    chunk.to_vec(),
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
            .map(|scope_json| canonical_json::to_string(&scope_json).unwrap())
            .collect::<Vec<_>>();

        let query_str = format!(
            r#"
            SELECT DISTINCT scope_data
            FROM flows
            WHERE
                scope_data IN ({})
            "#,
            sqlite_generate_placeholders_list(
                scope_json_parts.len(),
                NonZeroUsize::new(1).unwrap()
            )
        );

        let mut query = sqlx::query_scalar::<_, serde_json::Value>(&query_str);

        for scope_json in scope_json_parts {
            query = query.bind(scope_json);
        }

        let scope_data_vec = query.fetch_all(connection_mut).await.int_err()?;

        let filtered_flow_scopes = scope_data_vec
            .into_iter()
            .map(|scope_data| serde_json::from_value::<FlowScope>(scope_data).unwrap())
            .collect::<Vec<_>>();

        Ok(filtered_flow_scopes)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
