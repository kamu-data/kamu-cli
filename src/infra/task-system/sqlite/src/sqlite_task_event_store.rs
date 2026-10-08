// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use chrono::Utc;
use database_common::{
    PaginationOpts,
    TransactionRefT,
    sqlite_datetime_text,
    sqlite_generate_placeholders_list,
};
use dill::*;
use futures::TryStreamExt;
use kamu_task_system::*;
use serde_json::json;
use sqlx::Sqlite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn TaskEventStore)]
pub struct SqliteTaskEventStore {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

impl SqliteTaskEventStore {
    async fn register_task(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        task_id: TaskID,
        logical_plan: &LogicalPlan,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let task_id: i64 = task_id.try_into().unwrap();
        let maybe_dataset_id = logical_plan.dataset_id();
        let maybe_dataset_id_str = maybe_dataset_id.as_ref().map(ToString::to_string);

        sqlx::query!(
            r#"
            INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id)
                VALUES ($1, $2, 'queued', NULL)
            "#,
            task_id,
            maybe_dataset_id_str,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_task_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
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
                    SET task_status = CASE WHEN task_status = 'running' THEN $2 ELSE $5 END,
                        last_event_id = $3
                    WHERE task_id = $1 AND (
                        last_event_id IS NULL AND CAST($4 as INT8) IS NULL OR
                        last_event_id IS NOT NULL AND CAST($4 as INT8) IS NOT NULL AND last_event_id = $4
                    )
                    RETURNING task_id
                "#,
                event_task_id,
                status_if_running,
                last_event_id,
                maybe_prev_stored_event_id,
                status_otherwise,
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
        tr: &mut database_common::TransactionGuard<'_, Sqlite>,
        events: &[TaskEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        let events_json = serde_json::to_string(
            &events
                .iter()
                .map(|event| {
                    let event_task_id: i64 = event.task_id().try_into().unwrap();
                    Ok(json!({
                        "task_id": event_task_id,
                        "event_time": sqlite_datetime_text(&event.event_time()),
                        "event_type": event.typename(),
                        "event_payload": serde_json::to_value(event).int_err()?.to_string(),
                    }))
                })
                .collect::<Result<Vec<_>, InternalError>>()?,
        )
        .int_err()?;

        let event_ids = sqlx::query_scalar!(
            r#"
            INSERT INTO task_events (task_id, event_time, event_type, event_payload)
            SELECT value ->> 'task_id',
                   value ->> 'event_time',
                   value ->> 'event_type',
                   value ->> 'event_payload'
            FROM json_each($1)
            ORDER BY key
            RETURNING event_id
            "#,
            events_json,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let last_event_id = event_ids.into_iter().max().unwrap();
        Ok(EventID::new(last_event_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<TaskState> for SqliteTaskEventStore {
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, TaskEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let mut query_stream = sqlx::query_as!(
                EventRow,
                r#"
                SELECT event_id as "event_id: _", event_payload as "event_payload: _" FROM task_events
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

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let mut query_stream = sqlx::query_as!(
                EventRow,
                r#"
                SELECT event_id as "event_id: _", event_payload as "event_payload: _" FROM task_events
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


            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub task_id: i64,
                pub event_id: i64,
                pub event_payload: serde_json::Value,
            }

            let query_str = format!(
                r#"
                SELECT
                    task_id,
                    event_id,
                    event_payload
                FROM task_events
                    WHERE task_id IN ({})
                    ORDER BY event_id
                "#,
                sqlite_generate_placeholders_list(
                    task_ids.len(),
                    NonZeroUsize::new(1).unwrap()
                )
            );

            let mut query = sqlx::query_as::<_, EventRow>(&query_str);
            for task_id in task_ids {
                query = query.bind(task_id);
            }

            let mut query_stream = query
                .fetch(connection_mut)
                .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(event_row) = query_stream.try_next().await? {
                let task_id = TaskID::new(u64::try_from(event_row.task_id).unwrap());
                let event_id = EventID::new(event_row.event_id);
                let event = serde_json::from_value::<TaskEvent>(event_row.event_payload)
                    .map_err(|e| GetEventsError::Internal(e.int_err()))?;

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

        let mut new_tasks_json = Vec::new();
        let mut events_json = Vec::new();
        let mut item_event_counts = Vec::with_capacity(items.len());

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

                new_tasks_json.push(json!({
                    "task_id": task_id,
                    "dataset_id": e.logical_plan.dataset_id().as_ref().map(ToString::to_string),
                }));
            }

            item_event_counts.push(item.events.len());

            for event in &item.events {
                events_json.push(json!({
                    "task_id": task_id,
                    "event_time": sqlite_datetime_text(&event.event_time()),
                    "event_type": event.typename(),
                    "event_payload": serde_json::to_value(event).int_err()?.to_string(),
                }));
            }
        }

        let mut tr = self.transaction.lock().await;

        if !new_tasks_json.is_empty() {
            let new_tasks_json = serde_json::to_string(&new_tasks_json).int_err()?;

            let connection_mut = tr.connection_mut().await?;
            sqlx::query!(
                r#"
                INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id)
                    SELECT value ->> 'task_id', value ->> 'dataset_id', 'queued', NULL
                        FROM json_each($1)
                "#,
                new_tasks_json,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        let events_json = serde_json::to_string(&events_json).int_err()?;

        let connection_mut = tr.connection_mut().await?;
        let inserted_event_ids = sqlx::query_scalar!(
            r#"
            INSERT INTO task_events (task_id, event_time, event_type, event_payload)
            SELECT value ->> 'task_id',
                   value ->> 'event_time',
                   value ->> 'event_type',
                   value ->> 'event_payload'
            FROM json_each($1)
            ORDER BY key
            RETURNING event_id
            "#,
            events_json,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let last_event_ids = last_event_ids_per_item(inserted_event_ids, item_event_counts)?;

        let task_updates_json = serde_json::to_string(
            &items
                .iter()
                .zip(&last_event_ids)
                .map(|(item, last_event_id)| {
                    let task_id: i64 = item.query.try_into().unwrap();
                    // The stored status is unknown here: let the row pick the right outcome
                    let status_if_running: &'static str =
                        TaskEvent::status_after(&item.events, TaskStatus::Running).into();
                    let status_otherwise: &'static str =
                        TaskEvent::status_after(&item.events, TaskStatus::Queued).into();

                    json!({
                        "task_id": task_id,
                        "prev_event_id": item.maybe_prev_stored_event_id.map(EventID::into_inner),
                        "last_event_id": last_event_id.into_inner(),
                        "status_if_running": status_if_running,
                        "status_otherwise": status_otherwise,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .int_err()?;

        // Update denormalized task records: latest status and stored event.
        // A previously stored event id that does not match the expected one
        // leaves its row untouched, which means a concurrent modification
        let connection_mut = tr.connection_mut().await?;
        let updated_rows_count = sqlx::query!(
            r#"
            UPDATE tasks
                SET task_status = CASE
                        WHEN tasks.task_status = 'running' THEN u.value ->> 'status_if_running'
                        ELSE u.value ->> 'status_otherwise'
                    END,
                    last_event_id = u.value ->> 'last_event_id'
                FROM json_each($1) AS u
                WHERE tasks.task_id = u.value ->> 'task_id'
                    AND tasks.last_event_id IS (u.value ->> 'prev_event_id')
                RETURNING tasks.task_id
            "#,
            task_updates_json,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?
        .len();

        if updated_rows_count != items.len() {
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

        let count = usize::try_from(result.events_count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskEventStore for SqliteTaskEventStore {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
        pub struct NewTask {
            pub task_id: i64,
        }

        let created_time = Utc::now();

        let result = sqlx::query_as!(
            NewTask,
            r#"
            INSERT INTO task_ids(created_time) VALUES($1) RETURNING task_id as "task_id: _"
            "#,
            created_time
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(TaskID::try_from(result.task_id).unwrap())
    }

    async fn try_get_queued_task(&self) -> Result<Option<TaskID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_task_id = sqlx::query!(
            r#"
            SELECT task_id FROM tasks
                WHERE task_status = 'queued'
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
        .int_err()?;

        Ok(maybe_task_id)
    }

    /// Returns list of tasks, which are in Running state, from earliest to
    /// latest
    fn get_running_tasks(&self, pagination: PaginationOpts) -> TaskIDStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id FROM tasks
                    WHERE task_status == 'running'
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
                WHERE task_status == 'running'
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.tasks_count).int_err()?;
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
                SELECT task_id
                    FROM tasks
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

    /// Returns total number of tasks associated  with the specified dataset
    async fn get_count_tasks_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();

        let result = sqlx::query!(
            r#"
            SELECT COUNT(task_id) AS tasks_count FROM tasks
              WHERE dataset_id = $1
            "#,
            dataset_id_str
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.tasks_count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
