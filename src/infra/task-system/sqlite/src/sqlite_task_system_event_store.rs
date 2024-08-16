// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::{TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_task_system::*;
use opendatafabric::DatasetID;
use sqlx::{FromRow, QueryBuilder};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SqliteTaskSystemEventStore {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

#[component(pub)]
#[interface(dyn TaskSystemEventStore)]
impl SqliteTaskSystemEventStore {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<TaskState> for SqliteTaskSystemEventStore {
    async fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<TaskEvent> {
        let mut tr = self.transaction.lock().await;

        let task_id: i64 = (*task_id).into();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
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
                SELECT event_id as "event_id: _", event_payload as "event_payload: _" FROM task_events
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
        let connection_mut = tr.connection_mut().await?;

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO task_events (task_id, dataset_id, event_time, event_type, event_payload)
            "#,
        );

        query_builder.push_values(events, |mut b, event| {
            let event_task_id: i64 = event.task_id().into();
            b.push_bind(event_task_id);
            b.push_bind(event.dataset_id().map(ToString::to_string));
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

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) as count from task_events
            "#,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskSystemEventStore for SqliteTaskSystemEventStore {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
        #[allow(dead_code)]
        pub struct NewTask {
            pub task_id: i64,
        }

        let created_time = Utc::now();

        let result = sqlx::query_as!(
            NewTask,
            r#"
            INSERT INTO tasks(created_time) VALUES($1) RETURNING task_id as "task_id: _"
            "#,
            created_time
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(TaskID::new(result.task_id))
    }

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    async fn get_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
        pagination: TaskPaginationOpts,
    ) -> TaskIDStream {
        let mut tr = self.transaction.lock().await;
        let dataset_id = dataset_id.to_string();

        Box::pin(async_stream::stream! {
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id
                    FROM task_events
                    WHERE dataset_id = $1 AND event_type = 'TaskEventCreated'
                    ORDER  BY task_id DESC LIMIT $2 OFFSET $3
                "#,
                dataset_id,
                limit,
                offset,
            )
            .try_map(|event_row| Ok(TaskID::new(event_row.task_id)))
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
        dataset_id: &DatasetID,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id_str = dataset_id.to_string();

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) as count FROM task_events
              WHERE dataset_id = $1
            "#,
            dataset_id_str
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.count).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
