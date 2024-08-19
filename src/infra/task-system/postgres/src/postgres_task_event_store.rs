// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{PaginationOpts, TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_task_system::*;
use opendatafabric::DatasetID;
use sqlx::{FromRow, Postgres, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresTaskEventStore {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn TaskEventStore)]
impl PostgresTaskEventStore {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }

    async fn save_task_updates_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, Postgres>,
        events: &[TaskEvent],
    ) -> Result<(), SaveEventsError> {
        for event in events {
            let connection_mut = tr.connection_mut().await?;

            let event_task_id: i64 = (event.task_id()).try_into().unwrap();

            if let TaskEvent::TaskCreated(e) = &event {
                let maybe_dataset_id = e.logical_plan.dataset_id();
                sqlx::query!(
                    r#"
                    INSERT INTO tasks (task_id, dataset_id, task_status)
                        VALUES ($1, $2, $3)
                    "#,
                    event_task_id,
                    maybe_dataset_id.map(ToString::to_string),
                    TaskStatus::Queued as TaskStatus,
                )
                .execute(connection_mut)
                .await
                .map_err(|e| SaveEventsError::Internal(e.int_err()))?;
            }
            /* Existing task, update status */
            else {
                let new_status = event.new_status();

                sqlx::query!(
                    r#"
                    UPDATE tasks
                        SET task_status = $2
                        WHERE task_id = $1
                    "#,
                    event_task_id,
                    new_status as TaskStatus,
                )
                .execute(connection_mut)
                .await
                .map_err(|e| SaveEventsError::Internal(e.int_err()))?;
            }
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
    fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<TaskEvent> {
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

    async fn save_events(
        &self,
        _task_id: &TaskID,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        self.save_task_updates_from_events(&mut tr, &events).await?;
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        Ok(last_event_id)
    }

    async fn len(&self) -> Result<usize, InternalError> {
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

    /// Attempts to get the earliest queued task, if any
    async fn try_get_queued_task(&self) -> Result<Option<TaskID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_task_id = sqlx::query!(
            r#"
            SELECT task_id FROM tasks
                WHERE task_status = 'queued'::task_status_type
                ORDER BY task_id ASC
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
    fn get_running_tasks(&self, pagination: PaginationOpts) -> TaskIDStream {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id FROM tasks
                    WHERE task_status = 'running'::task_status_type
                    ORDER BY task_id ASC
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
        dataset_id: &DatasetID,
        pagination: PaginationOpts,
    ) -> TaskIDStream {
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
        dataset_id: &DatasetID,
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
