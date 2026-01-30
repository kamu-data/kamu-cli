// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::num::NonZeroUsize;

use database_common::{TransactionRefT, sqlite_generate_placeholders_list};
use dill::*;
use futures::TryStreamExt;
use internal_error::InternalError;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookSubscriptionEventStore)]
pub struct SqliteWebhookSubscriptionEventStore {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

impl SqliteWebhookSubscriptionEventStore {
    fn encode_event_types(event_types: &[WebhookEventType]) -> String {
        event_types
            .iter()
            .map(|t| t.as_ref().to_string())
            .collect::<Vec<_>>()
            .join(",")
    }

    async fn register_subscription(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Sqlite>,
        created_event: &WebhookSubscriptionEventCreated,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = created_event.subscription_id.as_ref();
        let maybe_dataset_id = created_event.dataset_id.as_ref().map(ToString::to_string);
        let event_types_as_str = Self::encode_event_types(&created_event.event_types);
        let label = if created_event.label.as_ref().is_empty() {
            None
        } else {
            Some(created_event.label.as_ref())
        };

        sqlx::query!(
            r#"
            INSERT INTO webhook_subscriptions (id, dataset_id, event_types, status, label)
                VALUES ($1, $2, $3, 'UNVERIFIED', $4)
            "#,
            subscription_id,
            maybe_dataset_id,
            event_types_as_str,
            label,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_subscription_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Sqlite>,
        events: &[WebhookSubscriptionEvent],
        maybe_prev_stored_event_id: Option<EventID>,
        last_event_id: EventID,
    ) -> Result<(), SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        let last_event_id: i64 = last_event_id.into();
        let maybe_prev_stored_event_id: Option<i64> = maybe_prev_stored_event_id.map(Into::into);

        let last_event = events.last().expect("Non empty event list expected");

        let event_subscription_id: &uuid::Uuid = last_event.subscription_id().as_ref();

        #[derive(sqlx::FromRow)]
        struct ResultRow {
            status: WebhookSubscriptionStatus,
        }

        let affected_rows =
            sqlx::query_as!(
                ResultRow,
                r#"
                UPDATE webhook_subscriptions
                    SET last_event_id = $2
                    WHERE id = $1 AND (
                        last_event_id IS NULL AND CAST($3 as BIGINT) IS NULL OR
                        last_event_id IS NOT NULL AND CAST($3 as BIGINT) IS NOT NULL AND last_event_id = $3
                    )
                    RETURNING status as "status: _"
                "#,
                event_subscription_id,
                last_event_id,
                maybe_prev_stored_event_id,
            )
            .fetch_all(connection_mut)
            .await
            .int_err()?;

        // If a previously stored event id does not match the expected,
        // this means we've just detected a concurrent modification (version conflict)
        if affected_rows.len() != 1 {
            return Err(SaveEventsError::concurrent_modification());
        }
        let affected_row = affected_rows.first().unwrap();

        // Compute the new status
        let mut new_status = affected_row.status;
        for event in events {
            new_status = event.new_status(new_status);
        }

        // If the status has changed, update it
        if new_status != affected_row.status {
            let connection_mut = tr.connection_mut().await?;

            sqlx::query!(
                r#"
                UPDATE webhook_subscriptions
                    SET status = $2
                    WHERE id = $1
                "#,
                event_subscription_id,
                new_status,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        // Compute new label and event list
        let mut updated_label = None;
        let mut updated_event_types = None;
        for event in events {
            if let WebhookSubscriptionEvent::Modified(e) = event {
                updated_label = Some(e.new_label.clone());
                updated_event_types = Some(e.new_event_types.clone());
            }
        }

        // If the label has changed, update it
        if let Some(updated_label) = updated_label {
            let connection_mut = tr.connection_mut().await?;

            let label_str = if updated_label.as_ref().is_empty() {
                None
            } else {
                Some(updated_label.as_ref())
            };

            sqlx::query!(
                r#"
                UPDATE webhook_subscriptions
                    SET label = $2
                    WHERE id = $1
                "#,
                event_subscription_id,
                label_str,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        // If the event types have changed, update them
        if let Some(updated_event_types) = updated_event_types {
            let connection_mut = tr.connection_mut().await?;

            let event_types_as_str = Self::encode_event_types(&updated_event_types);

            sqlx::query!(
                r#"
                UPDATE webhook_subscriptions
                    SET event_types = $2
                    WHERE id = $1
                "#,
                event_subscription_id,
                event_types_as_str,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Sqlite>,
        events: &[WebhookSubscriptionEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(sqlx::FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            INSERT INTO webhook_subscription_events (subscription_id, created_at, event_type, event_payload)
            "#,
        );

        query_builder.push_values(events, |mut b, event| {
            b.push_bind(event.subscription_id().as_ref());
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
impl EventStore<WebhookSubscriptionState> for SqliteWebhookSubscriptionEventStore {
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, WebhookSubscriptionEvent> {
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
                SELECT
                    event_id as "event_id: _",
                    event_payload as "event_payload: _"
                FROM webhook_subscription_events
                    WHERE
                        (cast($1 as INT8) IS NULL or event_id > $1) AND
                        (cast($2 as INT8) IS NULL or event_id <= $2)
                    ORDER BY event_id ASC
                "#,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = match serde_json::from_value::<WebhookSubscriptionEvent>(event_row.event_payload) {
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

    fn get_events(
        &self,
        subscription_id: &WebhookSubscriptionID,
        opts: GetEventsOpts,
    ) -> EventStream<'_, WebhookSubscriptionEvent> {
        let subscription_id = *subscription_id.as_ref();
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let mut query_stream = sqlx::query_as!(
                EventRow,
                r#"
                SELECT
                    event_id as "event_id: _",
                    event_payload as "event_payload: _"
                FROM webhook_subscription_events
                    WHERE subscription_id = $1
                         AND (cast($2 as INT8) IS NULL or event_id > $2)
                         AND (cast($3 as INT8) IS NULL or event_id <= $3)
                    ORDER BY event_id
                "#,
                subscription_id,
                maybe_from_id,
                maybe_to_id,
            ).try_map(|event_row| {
                let event = match serde_json::from_value::<WebhookSubscriptionEvent>(event_row.event_payload) {
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

    fn get_events_multi(
        &self,
        queries: &[WebhookSubscriptionID],
    ) -> MultiEventStream<'_, WebhookSubscriptionID, WebhookSubscriptionEvent> {
        let subscription_ids: Vec<uuid::Uuid> = queries.iter().map(|id| *id.as_ref()).collect();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            #[derive(Debug, sqlx::FromRow, PartialEq, Eq)]
            struct EventRow {
                pub subscription_id: uuid::Uuid,
                pub event_id: i64,
                pub event_payload: sqlx::types::JsonValue
            }

            let query_str = format!(
                r#"
                SELECT
                    subscription_id,
                    event_id,
                    event_payload
                FROM webhook_subscription_events
                    WHERE subscription_id IN ({})
                    ORDER BY event_id
                "#,
                sqlite_generate_placeholders_list(
                    subscription_ids.len(),
                    NonZeroUsize::new(1).unwrap()
                )
            );

            let mut query = sqlx::query_as::<_, EventRow>(&query_str);
            for subscription_id in subscription_ids {
                query = query.bind(subscription_id);
            }

            let mut query_stream = query
                .fetch(connection_mut)
                .map_err(|e| GetEventsError::Internal(e.int_err()));

            while let Some(event_row) = query_stream.try_next().await? {
                let subscription_id = WebhookSubscriptionID::new(event_row.subscription_id);
                let event_id = EventID::new(event_row.event_id);
                let event = serde_json::from_value::<WebhookSubscriptionEvent>(event_row.event_payload)
                    .map_err(|e| GetEventsError::Internal(e.int_err()))?;

                yield Ok((subscription_id, event_id, event));
            }
        })
    }

    async fn save_events(
        &self,
        subscription_id: &WebhookSubscriptionID,
        maybe_prev_stored_event_id: Option<EventID>,
        events: Vec<WebhookSubscriptionEvent>,
    ) -> Result<EventID, SaveEventsError> {
        // If there is nothing to save, exit quickly
        if events.is_empty() {
            return Err(SaveEventsError::NothingToSave);
        }

        let mut tr = self.transaction.lock().await;

        // For the newly created subscription, make sure it's registered before events
        let first_event = events.first().expect("Non empty event list expected");
        if let WebhookSubscriptionEvent::Created(e) = first_event {
            assert_eq!(subscription_id, &e.subscription_id);

            // When creating a subscription, there is no way something was already stored
            if maybe_prev_stored_event_id.is_some() {
                return Err(SaveEventsError::concurrent_modification());
            }

            // Make registration
            self.register_subscription(&mut tr, e).await?;
        }

        // Save events one by one
        let last_event_id = self.save_events_impl(&mut tr, &events).await?;

        // Update denormalized subscription record
        self.update_subscription_from_events(
            &mut tr,
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
            SELECT COUNT(event_id) AS events_count from webhook_subscription_events
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
impl WebhookSubscriptionEventStore for SqliteWebhookSubscriptionEventStore {
    async fn count_subscriptions_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<usize, CountWebhookSubscriptionsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();

        let result = sqlx::query!(
            r#"
            SELECT COUNT(id) AS subscriptions_count FROM webhook_subscriptions
                WHERE dataset_id = $1 AND status != 'REMOVED'
            "#,
            dataset_id,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.subscriptions_count).int_err()?;
        Ok(count)
    }

    async fn list_subscription_ids_by_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();

        let records = sqlx::query!(
            r#"
            SELECT id as "id: uuid::Uuid" FROM webhook_subscriptions
                WHERE dataset_id = $1 AND status != 'REMOVED'
            "#,
            dataset_id,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|record| WebhookSubscriptionID::new(record.id))
            .collect())
    }

    async fn list_all_subscription_ids(
        &self,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let records = sqlx::query!(
            r#"
            SELECT id as "id: uuid::Uuid" FROM webhook_subscriptions
                WHERE status != 'REMOVED'
            "#,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|record| WebhookSubscriptionID::new(record.id))
            .collect())
    }

    async fn find_subscription_id_by_dataset_and_label(
        &self,
        dataset_id: &odf::DatasetID,
        label: &WebhookSubscriptionLabel,
    ) -> Result<Option<WebhookSubscriptionID>, FindWebhookSubscriptionError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();
        let label = label.as_ref();

        let record = sqlx::query!(
            r#"
            SELECT id as "id: uuid::Uuid" FROM webhook_subscriptions
                WHERE dataset_id = $1 AND label = $2 AND status != 'REMOVED'
            "#,
            dataset_id,
            label,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(record.map(|record| WebhookSubscriptionID::new(record.id)))
    }

    async fn list_enabled_subscription_ids_by_dataset_and_event_type(
        &self,
        dataset_id: &odf::DatasetID,
        event_type: &WebhookEventType,
    ) -> Result<Vec<WebhookSubscriptionID>, ListWebhookSubscriptionsError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dataset_id = dataset_id.to_string();
        let event_type = event_type.as_ref().to_string();

        let records = sqlx::query!(
            r#"
            SELECT id as "id: uuid::Uuid" FROM webhook_subscriptions
                WHERE dataset_id = $1 AND
                      status = 'ENABLED' AND
                      event_types LIKE '%' || $2 || '%'
            "#,
            dataset_id,
            event_type,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(records
            .into_iter()
            .map(|record| WebhookSubscriptionID::new(record.id))
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
