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
use dill::{component, interface};
use event_sourcing::EventID;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    CreateResourceError,
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    UpdateResourceError,
};
use odf::metadata::AsStackString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceRepository)]
pub struct SqliteResourceRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for SqliteResourceRepository {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError> {
        Ok(ResourceID::new_v4())
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = resource_snapshot.account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);

        sqlx::query!(
            r#"
            INSERT INTO resources (
                resource_id,
                account_id,
                resource_kind,
                api_version,
                resource_name,
                description,
                labels,
                annotations,
                spec,
                status,
                generation,
                observed_generation,
                phase,
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            "#,
            resource_snapshot.resource_id,
            account_id_str,
            resource_snapshot.kind,
            resource_snapshot.api_version,
            resource_snapshot.name,
            resource_snapshot.description,
            resource_snapshot.labels,
            resource_snapshot.annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            resource_snapshot.generation,
            resource_snapshot.observed_generation,
            resource_snapshot.phase,
            resource_snapshot.created_at,
            resource_snapshot.updated_at,
            resource_snapshot.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateResourceError::Duplicate(kamu_resources::ResourceDuplicateError {
                    account_id: resource_snapshot.account_id.clone(),
                    kind: resource_snapshot.kind.clone(),
                    name: resource_snapshot.name.clone(),
                })
            }
            _ => CreateResourceError::Internal(e.int_err()),
        })?;

        Ok(())
    }

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = resource_snapshot.account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let expected_last_event_id = expected_last_event_id.map(EventID::into_inner);

        let update_result = sqlx::query!(
            r#"
            UPDATE resources
            SET
                account_id = $2,
                api_version = $3,
                resource_name = $4,
                description = $5,
                labels = $6,
                annotations = $7,
                spec = $8,
                status = $9,
                generation = $10,
                observed_generation = $11,
                phase = $12,
                updated_at = $13,
                deleted_at = $14,
                last_reconciled_at = $15,
                last_event_id = $16
            WHERE resource_id = $1
              AND (
                    last_event_id IS NULL AND CAST($17 as INT8) IS NULL OR
                    last_event_id IS NOT NULL AND CAST($17 as INT8) IS NOT NULL AND last_event_id = $17
              )
            "#,
            resource_snapshot.resource_id,
            account_id_str,
            resource_snapshot.api_version,
            resource_snapshot.name,
            resource_snapshot.description,
            resource_snapshot.labels,
            resource_snapshot.annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            resource_snapshot.generation,
            resource_snapshot.observed_generation,
            resource_snapshot.phase,
            resource_snapshot.updated_at,
            resource_snapshot.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
            expected_last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                UpdateResourceError::Duplicate(kamu_resources::ResourceDuplicateError {
                    account_id: resource_snapshot.account_id.clone(),
                    kind: resource_snapshot.kind.clone(),
                    name: resource_snapshot.name.clone(),
                })
            }
            _ => UpdateResourceError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(UpdateResourceError::concurrent_modification());
        }

        Ok(())
    }

    async fn get_resource_id_by_name(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let maybe_resource_id = sqlx::query_scalar!(
            r#"
            SELECT resource_id as "resource_id: uuid::Uuid"
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND resource_name = $3
            "#,
            account_id_str,
            kind,
            name,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_resource_id)
    }

    async fn get_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_row = sqlx::query!(
            r#"
            SELECT
                resource_id as "resource_id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                resource_kind,
                api_version,
                resource_name,
                description,
                labels as "labels: serde_json::Value",
                annotations as "annotations: serde_json::Value",
                spec as "spec: serde_json::Value",
                status as "status: serde_json::Value",
                generation,
                observed_generation,
                phase,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                deleted_at as "deleted_at: DateTime<Utc>",
                last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                last_event_id
            FROM resources
            WHERE resource_id = $1
              AND resource_kind = $2
            "#,
            query.id,
            query.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            resource_id: row.resource_id,
            account_id: row.account_id,
            kind: row.resource_kind,
            api_version: row.api_version,
            name: row.resource_name,
            description: row.description,
            labels: row.labels,
            annotations: row.annotations,
            spec: row.spec,
            status: row.status,
            generation: row.generation,
            observed_generation: row.observed_generation,
            phase: row.phase,
            created_at: row.created_at,
            updated_at: row.updated_at,
            deleted_at: row.deleted_at,
            last_reconciled_at: row.last_reconciled_at,
            last_event_id: row.last_event_id.map(event_sourcing::EventID::new),
        }))
    }

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        let resource_kind = kind.to_owned();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let account_id_stack = account_id.as_stack_string();
            let account_id_str = account_id_stack.as_str();
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT resource_id as "resource_id: uuid::Uuid"
                FROM resources
                WHERE account_id = $1
                  AND resource_kind = $2
                ORDER BY updated_at DESC, resource_id DESC
                LIMIT $3 OFFSET $4
                "#,
                account_id_str,
                resource_kind,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(row.resource_id);
            }
        })
    }

    async fn get_count_resources(
        &self,
        account_id: odf::AccountID,
        kind: &str,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!: i64"
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
            "#,
            account_id_str,
            kind,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(count).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
