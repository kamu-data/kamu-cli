// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_task_system::*;
use sqlx::{FromRow, Postgres, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn TaskEventStore)]
pub struct PostgresTaskEventStore {
    transaction: TransactionRefT<sqlx::Postgres>,
}

impl PostgresTaskEventStore {
    async fn register_task(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_id.try_into().unwrap();
        let maybe_dataset_id = logical_plan.dataset_id();

        sqlx::query!(
            r#"
            INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id)
                VALUES ($1, $2, 'queued'::task_status_type, NULL)
            "#,
            task_id,
            maybe_dataset_id.as_ref().map(ToString::to_string),
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_task_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        events: &[TaskEvent],
        maybe_prev_stored_event_id: Option<EventID>,
        last_event_id: EventID,
    ) -> Result<(), SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        let last_event_id: i64 = last_event_id.into();
        let maybe_prev_stored_event_id: Option<i64> = maybe_prev_stored_event_id.map(Into::into);

        let last_event = events.last().expect("Non empty event list expected");

        let event_task_id: i64 = (last_event.task_id()).try_into().unwrap();
        // The stored status is unknown here: let the row pick the right outcome
        let status_if_running = TaskEvent::status_after(events, TaskStatus::Running);
        let status_otherwise = TaskEvent::status_after(events, TaskStatus::Queued);

        let affected_rows_count =
            sqlx::query!(
                r#"
                UPDATE tasks
                    SET task_status = CASE
                            WHEN task_status = 'running'::task_status_type THEN $2::task_status_type
                            ELSE $5::task_status_type
                        END,
                        last_event_id = $3
                    WHERE task_id = $1 AND (
                        last_event_id IS NULL AND CAST($4 as BIGINT) IS NULL OR
                        last_event_id IS NOT NULL AND CAST($4 as BIGINT) IS NOT NULL AND last_event_id = $4
                    )
                    RETURNING task_id
                "#,
                event_task_id,
                status_if_running as TaskStatus,
                last_event_id,
                maybe_prev_stored_event_id,
                status_otherwise as TaskStatus,
            )
            .fetch_all(connection_mut)
            .await
            .int_err()?
            .len();

        // If a previously stored event id does not match the expected,
        // this means we've just detected a concurrent modification (version conflict)
        if affected_rows_count != 1 {
            return Err(SaveEventsError::concurrent_modification());
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        events: &[TaskEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = QueryBuilder::<sqlx::Postgres>::new(
            r#"
            INSERT INTO task_events (task_id, event_time, event_type, event_payload)
            "#,
        );

        query_builder.push_values(events, |mut b, event| {
            let event_task_id: i64 = (event.task_id()).try_into().unwrap();
            b.push_bind(event_task_id);
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
impl EventStore<TaskState> for PostgresTaskEventStore {
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, TaskEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload FROM task_events
                    WHERE
                         (cast($1 as INT8) IS NULL or event_id > $1) AND
                         (cast($2 as INT8) IS NULL or event_id <= $2)
                    ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = match serde_json::from_value::<TaskEvent>(event_row.event_payload) {
                    Ok(event) => event,
                    Err(e) => return Err(sqlx::Error::Decode(Box::new(e))),
                };
                Ok((EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield Ok((event_id, event));
            }
        })
    }

    fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<'_, TaskEvent> {
        let task_id: i64 = (*task_id).try_into().unwrap();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload FROM task_events
                    WHERE task_id = $1
                         AND (cast($2 as INT8) IS NULL or event_id > $2)
                         AND (cast($3 as INT8) IS NULL or event_id <= $3)
                    ORDER BY event_id
                "#,
                task_id,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = match serde_json::from_value::<TaskEvent>(event_row.event_payload) {
                    Ok(event) => event,
                    Err(e) => return Err(sqlx::Error::Decode(Box::new(e))),
                };
                Ok((EventID::new(event_row.event_id), event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield Ok((event_id, event));
            }
        })
    }

    fn get_events_multi(&self, queries: &[TaskID]) -> MultiEventStream<'_, TaskID, TaskEvent> {
        let task_ids: Vec<i64> = queries.iter().map(|id| (*id).try_into().unwrap()).collect();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id, event_id, event_payload
                FROM task_events
                WHERE task_id = ANY($1)
                ORDER BY event_id
                "#,
                &task_ids,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<TaskEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((TaskID::try_from(event_row.task_id).unwrap(), // ids are always > 0
                    EventID::new(event_row.event_id),
                    event))
            })
            .fetch(connection_mut)
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((task_id, event_id, event)) = query_stream.try_next().await? {
                yield Ok((task_id, event_id, event));
            }
        })
    }

    async fn save_events(
        &self,
        task_id: &TaskID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        // If there is nothing to save, exit quickly
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        // For the newly created task, make sure it's registered before events
        let first_event = events.first().expect("Non empty event list expected");
        if let TaskEvent::TaskCreated(e) = first_event {
            assert_eq!(task_id, &e.task_id);

            // When creating a task, there is no way something was already stored
            if maybe_prev_stored_event_id.is_some() {
                return Err(SaveEventsError::concurrent_modification());
            }

            // Make registration
            self.register_task(&mut tr, *task_id, &e.logical_plan)
                .await?;
        }

        // Save events one by one
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        // Update denormalized task record: latest status and stored event
        self.update_task_from_events(&mut tr, &events, maybe_prev_stored_event_id, last_event_id)
            .await?;

        Ok(last_event_id)
    }

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<TaskID, TaskEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        validate_multi_save_items(&items)?;

        let num_total_events = items.iter().map(|item| item.events.len()).sum();

        let mut new_task_ids = Vec::new();
        let mut new_task_dataset_ids = Vec::new();

        let mut item_task_ids = Vec::with_capacity(items.len());
        let mut item_prev_event_ids = Vec::with_capacity(items.len());
        let mut item_statuses_if_running = Vec::with_capacity(items.len());
        let mut item_statuses_otherwise = Vec::with_capacity(items.len());
        let mut item_event_counts = Vec::with_capacity(items.len());

        let mut event_task_ids = Vec::with_capacity(num_total_events);
        let mut event_times = Vec::with_capacity(num_total_events);
        let mut event_types = Vec::with_capacity(num_total_events);
        let mut event_payloads = Vec::with_capacity(num_total_events);

        for item in &items {
            let task_id: i64 = item.query.try_into().unwrap();

            // Newly created tasks must be registered before their events
            let first_event = item.events.first().expect("Non empty event list expected");
            if let TaskEvent::TaskCreated(e) = first_event {
                assert_eq!(item.query, e.task_id);

                // When creating a task, there is no way something was already stored
                if item.maybe_prev_stored_event_id.is_some() {
                    return Err(SaveEventsError::concurrent_modification());
                }

                new_task_ids.push(task_id);
                new_task_dataset_ids.push(
                    e.logical_plan
                        .dataset_id()
                        .as_ref()
                        .map(ToString::to_string),
                );
            }

            item_task_ids.push(task_id);
            item_prev_event_ids.push(item.maybe_prev_stored_event_id.map(EventID::into_inner));
            // The stored status is unknown here: let the row pick the right outcome
            item_statuses_if_running
                .push(TaskEvent::status_after(&item.events, TaskStatus::Running));
            item_statuses_otherwise.push(TaskEvent::status_after(&item.events, TaskStatus::Queued));
            item_event_counts.push(item.events.len());

            for event in &item.events {
                event_task_ids.push(task_id);
                event_times.push(event.event_time());
                event_types.push(event.typename());
                event_payloads.push(serde_json::to_value(event).int_err()?);
            }
        }

        let mut tr = self.transaction.lock().await;

        if !new_task_ids.is_empty() {
            let connection_mut = tr.connection_mut().await?;
            sqlx::query!(
                r#"
                INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id)
                    SELECT task_id, dataset_id, 'queued'::task_status_type, NULL
                        FROM UNNEST($1::bigint[], $2::text[]) AS t(task_id, dataset_id)
                "#,
                &new_task_ids,
                &new_task_dataset_ids as _,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        let connection_mut = tr.connection_mut().await?;
        let inserted_event_ids = sqlx::query_scalar!(
            r#"
            INSERT INTO task_events (task_id, event_time, event_type, event_payload)
                SELECT task_id, event_time, event_type, event_payload
                    FROM UNNEST($1::bigint[], $2::timestamptz[], $3::text[], $4::jsonb[])
                        WITH ORDINALITY AS e(task_id, event_time, event_type, event_payload, row_num)
                    ORDER BY row_num
                RETURNING event_id
            "#,
            &event_task_ids,
            &event_times,
            &event_types as _,
            &event_payloads,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let last_event_ids = last_event_ids_per_item(inserted_event_ids, item_event_counts)?;
        let item_last_event_ids: Vec<i64> = last_event_ids
            .iter()
            .copied()
            .map(EventID::into_inner)
            .collect();

        // Update denormalized task records: latest status and stored event.
        // A previously stored event id that does not match the expected one
        // leaves its row untouched, which means a concurrent modification
        let connection_mut = tr.connection_mut().await?;
        let updated_rows_count = sqlx::query!(
            r#"
            UPDATE tasks
                SET task_status = CASE
                        WHEN tasks.task_status = 'running'::task_status_type
                            THEN u.status_if_running
                        ELSE u.status_otherwise
                    END,
                    last_event_id = u.last_event_id
                FROM UNNEST($1::bigint[], $2::bigint[], $3::bigint[], $4::task_status_type[], $5::task_status_type[])
                    AS u(task_id, prev_event_id, last_event_id, status_if_running, status_otherwise)
                WHERE tasks.task_id = u.task_id
                    AND tasks.last_event_id IS NOT DISTINCT FROM u.prev_event_id
                RETURNING tasks.task_id
            "#,
            &item_task_ids,
            &item_prev_event_ids as _,
            &item_last_event_ids,
            &item_statuses_if_running as &[TaskStatus],
            &item_statuses_otherwise as &[TaskStatus],
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?
        .len();

        if updated_rows_count != item_task_ids.len() {
            return Err(SaveEventsError::concurrent_modification());
        }

        Ok(last_event_ids)
    }

    async fn total_events_stored(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) AS events_count from task_events
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
impl TaskEventStore for PostgresTaskEventStore {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT nextval('task_id_seq') AS new_task_id
            "#
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let task_id = result.new_task_id.unwrap();
        Ok(TaskID::try_from(task_id).unwrap())
    }

    async fn try_get_queued_task(&self) -> Result<Option<TaskID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_task_id = sqlx::query!(
            r#"
            SELECT task_id FROM tasks
                WHERE task_status = 'queued'::task_status_type
                ORDER BY task_id
                LIMIT 1
            "#,
        )
        .try_map(|event_row| {
            let task_id = event_row.task_id;
            Ok(TaskID::try_from(task_id).unwrap())
        })
        .fetch_optional(connection_mut)
        .await
        .map_err(ErrorIntoInternal::int_err)?;

        Ok(maybe_task_id)
    }

    /// Returns list of tasks, which are in Running state,
    /// from earliest to latest
    fn get_running_tasks(&self, pagination: PaginationOpts) -> TaskIDStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id FROM tasks
                    WHERE task_status = 'running'::task_status_type
                    ORDER BY task_id
                    LIMIT $1 OFFSET $2
                "#,
                limit,
                offset,
            )
            .try_map(|event_row| {
                let task_id = event_row.task_id;
                Ok(TaskID::try_from(task_id).unwrap())
            })
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(task_id) = query_stream.try_next().await? {
                yield Ok(task_id);
            }
        })
    }

    /// Returns total number of tasks, which are in Running state
    async fn get_count_running_tasks(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(task_id) AS tasks_count FROM tasks
                WHERE task_status = 'running'::task_status_type
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.tasks_count.unwrap()).int_err()?;
        Ok(count)
    }

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    fn get_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
        pagination: PaginationOpts,
    ) -> TaskIDStream<'_> {
        let dataset_id = dataset_id.to_string();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id FROM tasks
                    WHERE dataset_id = $1
                    ORDER BY task_id DESC
                    LIMIT $2 OFFSET $3
                "#,
                dataset_id,
                limit,
                offset,
            )
            .try_map(|event_row| {
                let task_id = event_row.task_id;
                Ok(TaskID::try_from(task_id).unwrap())
            })
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(task_id) = query_stream.try_next().await? {
                yield Ok(task_id);
            }
        })
    }

    /// Returns total number of tasks associated with the specified dataset
    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(task_id) AS tasks_count FROM tasks
                WHERE dataset_id = $1
            "#,
            dataset_id.to_string()
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.tasks_count.unwrap()).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
