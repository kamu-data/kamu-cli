// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::{TransactionRef, TransactionRefT};
use dill::*;
use futures::TryStreamExt;
use kamu_task_system::*;
use opendatafabric::DatasetID;
use sqlx::FromRow;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct TaskSystemEventStorePostgres {
    transaction: TransactionRefT<sqlx::Postgres>,
}

#[component(pub)]
#[interface(dyn TaskSystemEventStore)]
impl TaskSystemEventStorePostgres {
    pub fn new(transaction: TransactionRef) -> Self {
        Self {
            transaction: transaction.into(),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EventStore<TaskState> for TaskSystemEventStorePostgres {
    async fn get_events(&self, task_id: &TaskID, opts: GetEventsOpts) -> EventStream<TaskEvent> {
        let mut tr = self.transaction.lock().await;

        let task_id: i64 = (*task_id).into();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
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
            .fetch(tr.connection_mut())
            .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some((event_id, event)) = query_stream.try_next().await? {
                yield(Ok((event_id, event)));
            }
        })
    }

    async fn save_events(
        &self,
        _task_id: &TaskID,
        events: Vec<TaskEvent>,
    ) -> Result<EventID, SaveEventsError> {
        let mut tr = self.transaction.lock().await;

        #[derive(Default)]
        struct BulkInsertEvents {
            task_ids: Vec<i64>,
            dataset_ids: Vec<Option<String>>,
            event_times: Vec<DateTime<Utc>>,
            event_types: Vec<&'static str>,
            event_payloads: Vec<sqlx::types::JsonValue>,
        }

        let mut insert_events = BulkInsertEvents::default();
        for event in events {
            let event_task_id: i64 = (event.task_id()).into();

            insert_events.task_ids.push(event_task_id);
            insert_events
                .dataset_ids
                .push(event.dataset_id().map(ToString::to_string));
            insert_events.event_times.push(event.event_time());
            insert_events.event_types.push(event.typename());
            insert_events
                .event_payloads
                .push(serde_json::to_value(event).unwrap());
        }

        #[derive(FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let rows = sqlx::query_as!(
            ResultRow,
            r#"
            INSERT INTO task_events (task_id, dataset_id, event_time, event_type, event_payload)
                SELECT * FROM UNNEST(
                    $1::INT8[],
                    $2::TEXT[],
                    $3::timestamptz[],
                    $4::TEXT[],
                    $5::JSONB[]
                ) RETURNING event_id
            "#,
            &insert_events.task_ids,
            &insert_events.dataset_ids as _,
            &insert_events.event_times,
            &insert_events.event_types as _,
            &insert_events.event_payloads
        )
        .fetch_all(tr.connection_mut())
        .await
        .unwrap();

        if let Some(last_row) = rows.last() {
            Ok(EventID::new(last_row.event_id))
        } else {
            Ok(EventID::new(
                i64::try_from(self.len().await.unwrap()).unwrap(),
            ))
        }
    }

    async fn len(&self) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) from task_events
            "#,
        )
        .fetch_one(tr.connection_mut())
        .await
        .int_err()?;

        let count = usize::try_from(result.count.unwrap()).int_err()?;
        Ok(count)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskSystemEventStore for TaskSystemEventStorePostgres {
    /// Generates new unique task identifier
    async fn new_task_id(&self) -> Result<TaskID, InternalError> {
        let mut tr = self.transaction.lock().await;

        let result = sqlx::query!(
            r#"
            SELECT nextval('task_id_seq') as new_task_id
            "#
        )
        .fetch_one(tr.connection_mut())
        .await
        .int_err()?;

        Ok(TaskID::new(result.new_task_id.unwrap()))
    }

    /// Returns page of the tasks associated with the specified dataset in
    /// reverse chronological order based on creation time
    async fn get_tasks_by_dataset(
        &self,
        dataset_id: &DatasetID,
        pagination: TaskPaginationOpts,
    ) -> TaskIDStream {
        let mut tr = self.transaction.lock().await;
        let dataset_id = dataset_id.clone();

        Box::pin(async_stream::stream! {
            let mut query_stream = sqlx::query!(
                r#"
                SELECT task_id FROM
                (
                    SELECT DISTINCT ON (task_id) task_id, event_time
                        FROM task_events
                        WHERE dataset_id = $1
                        ORDER  BY task_id, event_time ASC
                )
                ORDER BY task_id DESC
                LIMIT $2 OFFSET $3
                "#,
                dataset_id.to_string(),
                i64::try_from(pagination.limit).unwrap(),
                i64::try_from(pagination.offset).unwrap(),
            )
            .try_map(|event_row| Ok(TaskID::new(event_row.task_id)))
            .fetch(tr.connection_mut())
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

        let result = sqlx::query!(
            r#"
            SELECT COUNT(event_id) FROM task_events
              WHERE dataset_id = $1
            "#,
            dataset_id.to_string()
        )
        .fetch_one(tr.connection_mut())
        .await
        .int_err()?;

        let count = usize::try_from(result.count.unwrap()).int_err()?;
        Ok(count)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
