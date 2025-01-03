// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use chrono::{DateTime, Utc};
use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_flow_system::*;
use opendatafabric::{AccountID, DatasetID};
use sqlx::{FromRow, Postgres, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const SYSTEM_INITIATOR: &str = "<system>";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresFlowEventStore {
    transaction: TransactionRefT<Postgres>,
}

#[component(pub)]
#[interface(dyn FlowEventStore)]
impl PostgresFlowEventStore {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

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
            .trigger
            .initiator_account_id()
            .map_or_else(|| SYSTEM_INITIATOR.to_string(), ToString::to_string);

        let flow_id: i64 = e.flow_id.try_into().unwrap();

        match &e.flow_key {
            FlowKey::Dataset(fk_dataset) => {
                sqlx::query!(
                r#"
                INSERT INTO flows (flow_id, dataset_id, dataset_flow_type, initiator, flow_status, last_event_id)
                    VALUES ($1, $2, $3, $4, 'waiting'::flow_status_type, NULL)
                "#,
                flow_id,
                fk_dataset.dataset_id.to_string(),
                fk_dataset.flow_type as DatasetFlowType,
                initiator,
            )
                .execute(connection_mut)
                .await
                .map_err(ErrorIntoInternal::int_err)?;
            }
            FlowKey::System(fk_system) => {
                sqlx::query!(
                    r#"
                INSERT INTO flows (flow_id, system_flow_type, initiator, flow_status, last_event_id)
                    VALUES ($1, $2, $3, 'waiting'::flow_status_type, NULL)
                "#,
                    flow_id,
                    fk_system.flow_type as SystemFlowType,
                    initiator,
                )
                .execute(connection_mut)
                .await
                .map_err(ErrorIntoInternal::int_err)?;
            }
        }

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
        for event in events {
            if let Some(new_status) = event.new_status() {
                maybe_latest_status = Some(new_status);
            }
            if let FlowEvent::ScheduledForActivation(e) = event {
                maybe_scheduled_for_activation_at = Some(e.scheduled_for_activation_at);
            } else if let FlowEvent::Aborted(_) | FlowEvent::TaskScheduled(_) = event {
                maybe_scheduled_for_activation_at = None;
            }
        }

        // We either have determined the latest status, or should read the previous
        let latest_status = if let Some(latest_status) = maybe_latest_status {
            latest_status
        } else {
            // Find out current flow status recorded
            let connection_mut = tr.connection_mut().await?;
            sqlx::query!(
                r#"
                SELECT flow_status as "flow_status: FlowStatus" FROM flows
                WHERE flow_id = $1
                "#,
                flow_id
            )
            .fetch_one(connection_mut)
            .await
            .map_err(|e| SaveEventsError::Internal(e.int_err()))?
            .flow_status
        };

        let connection_mut = tr.connection_mut().await?;
        let rows = sqlx::query!(
            r#"
            UPDATE flows
                SET flow_status = $2, last_event_id = $3, scheduled_for_activation_at = $4
                WHERE flow_id = $1 AND (
                    last_event_id IS NULL AND CAST($5 as BIGINT) IS NULL OR
                    last_event_id IS NOT NULL AND CAST($5 as BIGINT) IS NOT NULL AND last_event_id = $5
                )
                RETURNING flow_id
            "#,
            flow_id,
            latest_status as FlowStatus,
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

    async fn get_dataset_flow_run_stats(
        &self,
        dataset_id: &DatasetID,
        flow_type: DatasetFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let maybe_attempt_result = sqlx::query!(
            r#"
            SELECT attempt.last_event_time as last_attempt_time
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        f.dataset_id = $1 AND
                        f.dataset_flow_type = $2
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS attempt
            "#,
            dataset_id.to_string(),
            flow_type as DatasetFlowType
        )
        .map(|event_row| event_row.last_attempt_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let maybe_success_result = sqlx::query!(
            r#"
            SELECT success.last_event_time as last_success_time
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        e.event_payload::json#>'{TaskFinished,task_outcome,Success}' IS NOT NULL AND
                        f.dataset_id = $1 AND
                        f.dataset_flow_type = $2
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS success
            "#,
            dataset_id.to_string(),
            flow_type as DatasetFlowType
        )
        .map(|event_row| event_row.last_success_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(FlowRunStats {
            last_attempt_time: maybe_attempt_result,
            last_success_time: maybe_success_result,
        })
    }

    async fn get_system_flow_run_stats(
        &self,
        flow_type: SystemFlowType,
    ) -> Result<FlowRunStats, InternalError> {
        let mut tr = self.transaction.lock().await;

        let connection_mut = tr.connection_mut().await?;
        let maybe_attempt_result = sqlx::query!(
            r#"
            SELECT attempt.last_event_time as last_attempt_time
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        f.system_flow_type = $1
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS attempt
            "#,
            flow_type as SystemFlowType
        )
        .map(|event_row| event_row.last_attempt_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let maybe_success_result = sqlx::query!(
            r#"
            SELECT success.last_event_time as last_success_time
            FROM (
                SELECT e.event_id as event_id, e.event_time AS last_event_time
                    FROM flow_events e
                    INNER JOIN flows f ON f.flow_id = e.flow_id
                    WHERE
                        e.event_type = 'FlowEventTaskFinished' AND
                        e.event_payload::json#>'{TaskFinished,task_outcome,Success}' IS NOT NULL AND
                        f.system_flow_type = $1
                    ORDER BY e.event_id DESC
                    LIMIT 1
            ) AS success
            "#,
            flow_type as SystemFlowType
        )
        .map(|event_row| event_row.last_success_time)
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(FlowRunStats {
            last_attempt_time: maybe_attempt_result,
            last_success_time: maybe_success_result,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<FlowState> for PostgresFlowEventStore {
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
            self.register_flow(&mut tr, e)
                .await
                .map_err(SaveEventsError::Internal)?;
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
        flow_key: &FlowKey,
    ) -> Result<Option<FlowID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_flow_id = match flow_key {
            FlowKey::Dataset(flow_key_dataset) => sqlx::query!(
                r#"
                SELECT flow_id FROM flows
                    WHERE dataset_id = $1 AND
                          dataset_flow_type = $2 AND
                          flow_status != 'finished'::flow_status_type
                    ORDER BY flow_id DESC
                    LIMIT 1
                "#,
                flow_key_dataset.dataset_id.to_string(),
                flow_key_dataset.flow_type as DatasetFlowType,
            )
            .fetch_optional(connection_mut)
            .await
            .int_err()?
            .map(|r| r.flow_id),

            FlowKey::System(flow_key_system) => sqlx::query!(
                r#"
                SELECT flow_id FROM flows
                    WHERE system_flow_type = $1 AND
                          flow_status != 'finished'::flow_status_type
                    ORDER BY flow_id DESC
                    LIMIT 1
                "#,
                flow_key_system.flow_type as SystemFlowType,
            )
            .fetch_optional(connection_mut)
            .await
            .int_err()?
            .map(|r| r.flow_id),
        };

        Ok(maybe_flow_id.map(|id| FlowID::try_from(id).unwrap()))
    }

    async fn get_flow_run_stats(&self, flow_key: &FlowKey) -> Result<FlowRunStats, InternalError> {
        match flow_key {
            FlowKey::Dataset(dataset_flow_key) => {
                self.get_dataset_flow_run_stats(
                    &dataset_flow_key.dataset_id,
                    dataset_flow_key.flow_type,
                )
                .await
            }
            FlowKey::System(system_flow_key) => {
                self.get_system_flow_run_stats(system_flow_key.flow_type)
                    .await
            }
        }
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
                    f.flow_status = 'waiting'::flow_status_type
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
                    f.flow_status = 'waiting'::flow_status_type
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

    fn get_all_flow_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_id = dataset_id.to_string();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let by_flow_type = filters.by_flow_type;
        let by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                WITH unsorted_flows AS
                    (SELECT
                        f.flow_id,
                        f.flow_status,
                        MAX(e.event_time) as last_event_time
                    FROM flows f
                    LEFT JOIN flow_events e USING(flow_id)
                    WHERE
                        f.dataset_id = $1
                        AND (cast($2 as dataset_flow_type) IS NULL OR f.dataset_flow_type = $2)
                        AND (cast($3 as flow_status_type) IS NULL OR f.flow_status = $3)
                        AND (cast($4 as TEXT[]) IS NULL OR f.initiator = ANY($4))
                    GROUP BY f.flow_id, f.flow_status)
                SELECT flow_id FROM unsorted_flows
                ORDER BY flow_status, last_event_time DESC
                LIMIT $5 OFFSET $6
                "#,
               dataset_id,
               by_flow_type as Option<DatasetFlowType>,
               by_flow_status as Option<FlowStatus>,
               maybe_initiators as Option<Vec<String>>,
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

    async fn get_count_flows_by_dataset(
        &self,
        dataset_id: &DatasetID,
        filters: &DatasetFlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_result = sqlx::query!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE dataset_id = $1
                AND (cast($2 as dataset_flow_type) IS NULL OR dataset_flow_type = $2)
                AND (cast($3 as flow_status_type) IS NULL OR flow_status = $3)
                AND (cast($4 as TEXT[]) IS NULL OR initiator = ANY($4))
            "#,
            dataset_id.to_string(),
            filters.by_flow_type as Option<DatasetFlowType>,
            filters.by_flow_status as Option<FlowStatus>,
            maybe_initiators as Option<Vec<String>>
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let flows_count = query_result.flows_count.unwrap_or_default();
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_all_flow_ids_by_datasets(
        &self,
        dataset_ids: HashSet<DatasetID>,
        filters: &DatasetFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let dataset_ids: Vec<_> = dataset_ids.iter().map(ToString::to_string).collect();

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let by_flow_type = filters.by_flow_type;
        let by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                WITH unsorted_flows AS
                    (SELECT
                        f.flow_id,
                        f.flow_status,
                        MAX(e.event_time) as last_event_time
                    FROM flows f
                    LEFT JOIN flow_events e USING(flow_id)
                    WHERE
                        f.dataset_id = ANY($1)
                        AND (cast($2 as dataset_flow_type) IS NULL OR f.dataset_flow_type = $2)
                        AND (cast($3 as flow_status_type) IS NULL OR f.flow_status = $3)
                        AND (cast($4 as TEXT[]) IS NULL OR f.initiator = ANY($4))
                    GROUP BY f.flow_id, f.flow_status)
                SELECT flow_id FROM unsorted_flows
                ORDER BY flow_status, last_event_time DESC
                LIMIT $5 OFFSET $6
                "#,
                dataset_ids as Vec<String>,
                by_flow_type as Option<DatasetFlowType>,
                by_flow_status as Option<FlowStatus>,
                maybe_initiators as Option<Vec<String>>,
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

    fn get_unique_flow_initiator_ids_by_dataset(
        &self,
        dataset_id: &DatasetID,
    ) -> InitiatorIDStream {
        let dataset_id = dataset_id.to_string();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT DISTINCT(initiator) FROM flows
                    WHERE dataset_id = $1 AND initiator != $2
                "#,
                dataset_id,
                SYSTEM_INITIATOR,
            ).try_map(|event_row| {
                Ok(AccountID::from_did_str(&event_row.initiator).unwrap())
            })
            .fetch(connection_mut);

            while let Some(initiator) = query_stream.try_next().await.int_err()? {
                yield Ok(initiator);
            }
        })
    }

    fn get_all_system_flow_ids(
        &self,
        filters: &SystemFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_type = filters.by_flow_type;
        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT flow_id FROM flows
                    WHERE system_flow_type IS NOT NULL
                    AND (cast($1 as system_flow_type) IS NULL OR system_flow_type = $1)
                    AND (cast($2 as flow_status_type) IS NULL or flow_status = $2)
                    AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
                ORDER BY flow_id DESC
                LIMIT $4 OFFSET $5
                "#,
                maybe_by_flow_type as Option<SystemFlowType>,
                maybe_by_flow_status as Option<FlowStatus>,
                maybe_initiators as Option<Vec<String>>,
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

    async fn get_count_system_flows(
        &self,
        filters: &SystemFlowFilters,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let query_result = sqlx::query!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE system_flow_type IS NOT NULL
                AND (cast($1 as system_flow_type) IS NULL OR system_flow_type = $1)
                AND (cast($2 as flow_status_type) IS NULL or flow_status = $2)
                AND (cast($3 as TEXT[]) IS NULL OR initiator = ANY($3))
            "#,
            filters.by_flow_type as Option<SystemFlowType>,
            filters.by_flow_status as Option<FlowStatus>,
            maybe_initiators as Option<Vec<String>>,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let flows_count = query_result.flows_count.unwrap_or_default();
        Ok(usize::try_from(flows_count).unwrap())
    }

    fn get_all_flow_ids(
        &self,
        filters: &AllFlowFilters,
        pagination: PaginationOpts,
    ) -> FlowIDStream<'_> {
        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_status = filters.by_flow_status;

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;

            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT flow_id FROM flows
                WHERE
                    (cast($1 as flow_status_type) IS NULL or flow_status = $1)
                    AND (cast($2 as TEXT[]) IS NULL OR initiator = ANY($2))
                ORDER BY flow_id DESC
                LIMIT $3 OFFSET $4
                "#,
                maybe_by_flow_status as Option<FlowStatus>,
                maybe_initiators as Option<Vec<String>>,
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

    async fn get_count_all_flows(&self, filters: &AllFlowFilters) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_initiators = filters
            .by_initiator
            .as_ref()
            .map(Self::prepare_initiator_filter);

        let maybe_by_flow_status = filters.by_flow_status;

        let query_result = sqlx::query!(
            r#"
            SELECT COUNT(flow_id) AS flows_count
            FROM flows
                WHERE
                    (cast($1 as flow_status_type) IS NULL or flow_status = $1)
                    AND (cast($2 as TEXT[]) IS NULL OR initiator = ANY($2))
            "#,
            maybe_by_flow_status as Option<FlowStatus>,
            maybe_initiators as Option<Vec<String>>,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let flows_count = query_result.flows_count.unwrap_or_default();
        Ok(usize::try_from(flows_count).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
