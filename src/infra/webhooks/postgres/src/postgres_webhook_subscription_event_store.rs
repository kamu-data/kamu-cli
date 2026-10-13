// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use database_common::TransactionRefT;
use dill::*;
use futures::TryStreamExt;
use internal_error::InternalError;
use kamu_webhooks::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn WebhookSubscriptionEventStore)]
pub struct PostgresWebhookSubscriptionEventStore {
    transaction: TransactionRefT<sqlx::Postgres>,
}

impl PostgresWebhookSubscriptionEventStore {
    async fn register_subscription(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Postgres>,
        created_event: &WebhookSubscriptionEventCreated,
    ) -> Result<(), InternalError> {
        let connection_mut = tr.connection_mut().await?;

        let subscription_id: &uuid::Uuid = created_event.subscription_id.as_ref();
        let maybe_dataset_id = created_event.dataset_id.as_ref().map(ToString::to_string);
        let event_types = created_event
            .event_types
            .iter()
            .map(|t| t.as_ref().to_string())
            .collect::<Vec<_>>();
        let label = if created_event.label.as_ref().is_empty() {
            None
        } else {
            Some(created_event.label.as_ref())
        };

        sqlx::query!(
            r#"
            INSERT INTO webhook_subscriptions (id, dataset_id, event_types, status, label)
                VALUES ($1, $2, $3, 'UNVERIFIED'::webhook_subscription_status, $4)
            "#,
            subscription_id,
            maybe_dataset_id,
            &event_types,
            label,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    async fn update_subscription_from_events(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Postgres>,
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
                new_status as WebhookSubscriptionStatus,
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

            let label = if updated_label.as_ref().is_empty() {
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
                label,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        // If the event types have changed, update them
        if let Some(updated_event_types) = updated_event_types {
            let connection_mut = tr.connection_mut().await?;

            let updated_event_types = updated_event_types
                .iter()
                .map(|t| t.as_ref().to_string())
                .collect::<Vec<_>>();

            sqlx::query!(
                r#"
                UPDATE webhook_subscriptions
                    SET event_types = $2
                    WHERE id = $1
                "#,
                event_subscription_id,
                &updated_event_types,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        Ok(())
    }

    async fn save_events_impl(
        &self,
        tr: &mut database_common::TransactionGuard<'_, sqlx::Postgres>,
        events: &[WebhookSubscriptionEvent],
    ) -> Result<EventID, SaveEventsError> {
        let connection_mut = tr.connection_mut().await?;

        #[derive(sqlx::FromRow)]
        struct ResultRow {
            event_id: i64,
        }

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
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
impl EventStore<WebhookSubscriptionState> for PostgresWebhookSubscriptionEventStore {
    fn get_all_events(&self, opts: GetEventsOpts) -> EventStream<'_, WebhookSubscriptionEvent> {
        let maybe_from_id = opts.from.map(EventID::into_inner);
        let maybe_to_id = opts.to.map(EventID::into_inner);

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload FROM webhook_subscription_events
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
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT event_id, event_payload FROM webhook_subscription_events
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
            let connection_mut = tr
                .connection_mut()
                .await?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT
                    subscription_id, event_id, event_payload
                FROM webhook_subscription_events
                    WHERE subscription_id = ANY($1)
                    ORDER BY event_id
                "#,
                &subscription_ids,
            ).try_map(|event_row| {
                let event = serde_json::from_value::<WebhookSubscriptionEvent>(event_row.event_payload)
                    .map_err(|e| sqlx::Error::Decode(Box::new(e)))?;

                Ok((WebhookSubscriptionID::new(event_row.subscription_id),
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

    async fn save_events_multi(
        &self,
        items: Vec<SaveEventsItem<WebhookSubscriptionID, WebhookSubscriptionEvent>>,
    ) -> Result<Vec<EventID>, SaveEventsError> {
        if items.is_empty() {
            return Ok(vec![]);
        }

        validate_multi_save_items(&items)?;

        let num_total_events = items.iter().map(|item| item.events.len()).sum();

        let mut new_subscription_ids = Vec::new();
        let mut new_subscription_dataset_ids = Vec::new();
        let mut new_subscription_event_types = Vec::new();
        let mut new_subscription_labels = Vec::new();

        let mut item_subscription_ids = Vec::with_capacity(items.len());
        let mut item_prev_event_ids = Vec::with_capacity(items.len());
        let mut item_event_counts = Vec::with_capacity(items.len());

        let mut event_subscription_ids = Vec::with_capacity(num_total_events);
        let mut event_times = Vec::with_capacity(num_total_events);
        let mut event_types = Vec::with_capacity(num_total_events);
        let mut event_payloads = Vec::with_capacity(num_total_events);

        for item in &items {
            let subscription_id: uuid::Uuid = *item.query.as_ref();

            // Newly created subscriptions must be registered before their events
            let first_event = item.events.first().expect("Non empty event list expected");
            if let WebhookSubscriptionEvent::Created(e) = first_event {
                assert_eq!(item.query, e.subscription_id);

                // When creating a subscription, there is no way something was already stored
                if item.maybe_prev_stored_event_id.is_some() {
                    return Err(SaveEventsError::concurrent_modification());
                }

                new_subscription_ids.push(subscription_id);
                new_subscription_dataset_ids.push(e.dataset_id.as_ref().map(ToString::to_string));
                new_subscription_event_types.push(event_types_to_json(&e.event_types));
                new_subscription_labels.push(label_to_column(&e.label));
            }

            item_subscription_ids.push(subscription_id);
            item_prev_event_ids.push(item.maybe_prev_stored_event_id.map(EventID::into_inner));
            item_event_counts.push(item.events.len());

            for event in &item.events {
                event_subscription_ids.push(subscription_id);
                event_times.push(event.event_time());
                event_types.push(event.typename());
                event_payloads.push(serde_json::to_value(event).int_err()?);
            }
        }

        let mut tr = self.transaction.lock().await;

        if !new_subscription_ids.is_empty() {
            let connection_mut = tr.connection_mut().await?;
            sqlx::query!(
                r#"
                INSERT INTO webhook_subscriptions (id, dataset_id, event_types, status, label)
                    SELECT
                        u.id,
                        u.dataset_id,
                        ARRAY(SELECT jsonb_array_elements_text(u.event_types)),
                        'UNVERIFIED'::webhook_subscription_status,
                        u.label
                    FROM UNNEST($1::uuid[], $2::text[], $3::jsonb[], $4::text[])
                        AS u(id, dataset_id, event_types, label)
                "#,
                &new_subscription_ids,
                &new_subscription_dataset_ids as _,
                &new_subscription_event_types,
                &new_subscription_labels as _,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        let connection_mut = tr.connection_mut().await?;
        let inserted_event_ids = sqlx::query_scalar!(
            r#"
            INSERT INTO webhook_subscription_events (subscription_id, created_at, event_type, event_payload)
                SELECT subscription_id, created_at, event_type, event_payload
                    FROM UNNEST($1::uuid[], $2::timestamptz[], $3::text[], $4::jsonb[])
                        WITH ORDINALITY AS e(subscription_id, created_at, event_type, event_payload, row_num)
                    ORDER BY row_num
                RETURNING event_id
            "#,
            &event_subscription_ids,
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

        // A previously stored event id that does not match the expected one
        // leaves its row untouched, which means a concurrent modification
        let connection_mut = tr.connection_mut().await?;
        let updated_rows = sqlx::query!(
            r#"
            UPDATE webhook_subscriptions
                SET last_event_id = u.last_event_id
                FROM UNNEST($1::uuid[], $2::bigint[], $3::bigint[])
                    AS u(id, prev_event_id, last_event_id)
                WHERE webhook_subscriptions.id = u.id
                    AND webhook_subscriptions.last_event_id IS NOT DISTINCT FROM u.prev_event_id
                RETURNING webhook_subscriptions.id, webhook_subscriptions.status AS "status: WebhookSubscriptionStatus"
            "#,
            &item_subscription_ids,
            &item_prev_event_ids as _,
            &item_last_event_ids,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        if updated_rows.len() != items.len() {
            return Err(SaveEventsError::concurrent_modification());
        }

        // Update the rest of denormalized subscription records
        let stored_statuses: HashMap<uuid::Uuid, WebhookSubscriptionStatus> = updated_rows
            .into_iter()
            .map(|row| (row.id, row.status))
            .collect();

        let mut changed_subscription_ids = Vec::new();
        let mut changed_statuses = Vec::new();
        let mut changed_label_flags = Vec::new();
        let mut changed_labels = Vec::new();
        let mut changed_event_types_flags = Vec::new();
        let mut changed_event_types = Vec::new();

        for (item, subscription_id) in items.iter().zip(&item_subscription_ids) {
            let stored_status = stored_statuses[subscription_id];
            let new_status = item
                .events
                .iter()
                .fold(stored_status, |status, event| event.new_status(status));

            let maybe_modified = item.events.iter().rev().find_map(|event| match event {
                WebhookSubscriptionEvent::Modified(e) => Some(e),
                WebhookSubscriptionEvent::Created(_)
                | WebhookSubscriptionEvent::Enabled(_)
                | WebhookSubscriptionEvent::Paused(_)
                | WebhookSubscriptionEvent::Resumed(_)
                | WebhookSubscriptionEvent::MarkedUnreachable(_)
                | WebhookSubscriptionEvent::Reactivated(_)
                | WebhookSubscriptionEvent::SecretRotated(_)
                | WebhookSubscriptionEvent::Removed(_) => None,
            });

            if new_status == stored_status && maybe_modified.is_none() {
                continue;
            }

            changed_subscription_ids.push(*subscription_id);
            changed_statuses.push(new_status);
            changed_label_flags.push(maybe_modified.is_some());
            changed_labels.push(maybe_modified.and_then(|e| label_to_column(&e.new_label)));
            changed_event_types_flags.push(maybe_modified.is_some());
            changed_event_types.push(
                maybe_modified.map_or(serde_json::Value::Array(vec![]), |e| {
                    event_types_to_json(&e.new_event_types)
                }),
            );
        }

        if !changed_subscription_ids.is_empty() {
            let connection_mut = tr.connection_mut().await?;
            sqlx::query!(
                r#"
                UPDATE webhook_subscriptions
                    SET status = u.status,
                        label = CASE WHEN u.label_changed THEN u.label ELSE webhook_subscriptions.label END,
                        event_types = CASE
                            WHEN u.event_types_changed THEN ARRAY(SELECT jsonb_array_elements_text(u.event_types))
                            ELSE webhook_subscriptions.event_types
                        END
                    FROM UNNEST(
                        $1::uuid[],
                        $2::webhook_subscription_status[],
                        $3::bool[],
                        $4::text[],
                        $5::bool[],
                        $6::jsonb[]
                    ) AS u(id, status, label_changed, label, event_types_changed, event_types)
                    WHERE webhook_subscriptions.id = u.id
                "#,
                &changed_subscription_ids,
                &changed_statuses as &[WebhookSubscriptionStatus],
                &changed_label_flags,
                &changed_labels as _,
                &changed_event_types_flags,
                &changed_event_types,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        Ok(last_event_ids)
    }

    async fn total_events_stored(&self) -> Result<usize, InternalError> {
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

        let count = usize::try_from(result.events_count.unwrap()).int_err()?;
        Ok(count)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl WebhookSubscriptionEventStore for PostgresWebhookSubscriptionEventStore {
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
                WHERE
                    dataset_id = $1
                    AND status != 'REMOVED'::webhook_subscription_status
            "#,
            dataset_id,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        let count = usize::try_from(result.subscriptions_count.unwrap()).int_err()?;
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
            SELECT id FROM webhook_subscriptions
                WHERE
                    dataset_id = $1
                    AND status != 'REMOVED'::webhook_subscription_status
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
            SELECT id FROM webhook_subscriptions
                WHERE status != 'REMOVED'::webhook_subscription_status
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
            SELECT id FROM webhook_subscriptions
                WHERE dataset_id = $1 AND label = $2 AND status != 'REMOVED'::webhook_subscription_status
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
            SELECT id FROM webhook_subscriptions
                WHERE dataset_id = $1 AND
                    status = 'ENABLED'::webhook_subscription_status AND
                    event_types::text[] @> $2::text[]
            "#,
            dataset_id,
            &vec![event_type],
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

fn event_types_to_json(event_types: &[WebhookEventType]) -> serde_json::Value {
    serde_json::Value::Array(
        event_types
            .iter()
            .map(|t| serde_json::Value::String(t.as_ref().to_string()))
            .collect(),
    )
}

/// Empty labels are stored as NULL
fn label_to_column(label: &WebhookSubscriptionLabel) -> Option<String> {
    if label.as_ref().is_empty() {
        None
    } else {
        Some(label.as_ref().to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
