// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::num::NonZeroUsize;

use chrono::{DateTime, Utc};
use database_common::{
    PaginationOpts,
    TransactionRefT,
    sql_like_escape_pattern,
    sqlite_generate_placeholders_list,
};
use dill::{component, interface};
use event_sourcing::EventID;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    CreateResourceError,
    ResourceDuplicateError,
    ResourceHeaders,
    ResourceID,
    ResourceIDStream,
    ResourceIdentityRow,
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotRow,
    ResourceSnapshotStream,
    ResourceSummaryRow,
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
        Ok(ResourceID::new(uuid::Uuid::new_v4()))
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = resource_snapshot.headers.account.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let name_str = resource_snapshot.headers.name.as_str();
        let labels = serde_json::to_value(&resource_snapshot.headers.labels).unwrap();
        let annotations = serde_json::to_value(&resource_snapshot.headers.annotations).unwrap();
        let generation = i64::try_from(resource_snapshot.headers.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let resource_id: &uuid::Uuid = resource_snapshot.id.as_ref();

        sqlx::query!(
            r#"
            INSERT INTO resources (
                resource_id,
                account_id,
                resource_schema,
                resource_name,
                description,
                labels,
                annotations,
                spec,
                status,
                generation,
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            "#,
            resource_id,
            account_id_str,
            resource_snapshot.schema,
            name_str,
            resource_snapshot.headers.description,
            labels,
            annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            generation,
            resource_snapshot.headers.created_at,
            resource_snapshot.headers.updated_at,
            resource_snapshot.headers.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.headers.account.clone(),
                    schema: resource_snapshot.schema.clone(),
                    name: resource_snapshot.headers.name.clone(),
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

        let account_id_stack = resource_snapshot.headers.account.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let name_str = resource_snapshot.headers.name.as_str();
        let labels = serde_json::to_value(&resource_snapshot.headers.labels).unwrap();
        let annotations = serde_json::to_value(&resource_snapshot.headers.annotations).unwrap();
        let generation = i64::try_from(resource_snapshot.headers.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let expected_last_event_id = expected_last_event_id.map(EventID::into_inner);
        let resource_id: &uuid::Uuid = resource_snapshot.id.as_ref();

        let update_result = sqlx::query!(
            r#"
            UPDATE resources
            SET
                account_id = $2,
                resource_schema = $3,
                resource_name = $4,
                description = $5,
                labels = $6,
                annotations = $7,
                spec = $8,
                status = $9,
                generation = $10,
                updated_at = $11,
                deleted_at = $12,
                last_reconciled_at = $13,
                last_event_id = $14
            WHERE resource_id = $1
              AND (
                    last_event_id IS NULL AND CAST($15 as INT8) IS NULL OR
                    last_event_id IS NOT NULL AND CAST($15 as INT8) IS NOT NULL AND last_event_id = $15
              )
            "#,
            resource_id,
            account_id_str,
            resource_snapshot.schema,
            name_str,
            resource_snapshot.headers.description,
            labels,
            annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            generation,
            resource_snapshot.headers.updated_at,
            resource_snapshot.headers.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
            expected_last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                UpdateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.headers.account.clone(),
                    schema: resource_snapshot.schema.clone(),
                    name: resource_snapshot.headers.name.clone(),
                })
            }
            _ => UpdateResourceError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(UpdateResourceError::concurrent_modification());
        }

        Ok(())
    }

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let name_str = name.as_str();

        let maybe_resource_id = sqlx::query_scalar!(
            r#"
            SELECT resource_id as "id: uuid::Uuid"
            FROM resources
            WHERE account_id = $1
              AND resource_schema = $2
              AND resource_name COLLATE NOCASE = $3
              AND deleted_at IS NULL
            "#,
            account_id_str,
            schema,
            name_str,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_resource_id.map(ResourceID::new))
    }

    async fn find_resource_identities_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let uids_placeholders =
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_id as id,
                resource_schema as schema,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_id IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceIdentityRow>(&query_str).bind(account_id_str);
        for id in ids {
            query = query.bind(*id.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows)
    }

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let names_placeholders =
            sqlite_generate_placeholders_list(names.len(), NonZeroUsize::new(3).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_id as id,
                resource_schema as schema,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_schema = $2
              AND resource_name COLLATE NOCASE IN ({names_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceIdentityRow>(&query_str)
            .bind(account_id_str)
            .bind(schema);
        for name in names {
            query = query.bind(name.to_string());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows)
    }

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let limit = i64::try_from(pagination.limit).int_err()?;
        let offset = i64::try_from(pagination.offset).int_err()?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT
                resource_id as id,
                resource_schema as schema,
                resource_name as name
            FROM resources
            WHERE account_id = "#,
        );
        query_builder
            .push_bind(account_id_str)
            .push(" AND deleted_at IS NULL AND resource_schema IN (");
        {
            let mut separated = query_builder.separated(", ");
            for schema in schemas {
                separated.push_bind(schema);
            }
        }
        query_builder.push(")");

        if let Some(exact_names) = exact_names {
            query_builder.push(" AND resource_name COLLATE NOCASE IN (");
            let mut separated = query_builder.separated(", ");
            for name in exact_names {
                separated.push_bind(name.to_string());
            }
            query_builder.push(")");
        }

        if let Some(name_pattern) = name_pattern {
            query_builder.push(" AND resource_name LIKE ");
            query_builder.push_bind(sql_like_escape_pattern(name_pattern));
            query_builder.push(r#" ESCAPE '\' COLLATE NOCASE"#);
        }

        query_builder
            .push(" ORDER BY updated_at DESC, resource_id DESC LIMIT ")
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(offset);

        query_builder
            .build_query_as::<ResourceIdentityRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()
    }

    async fn count_search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError> {
        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(0);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT COUNT(*) as count
            FROM resources
            WHERE account_id = "#,
        );
        query_builder
            .push_bind(account_id_str)
            .push(" AND deleted_at IS NULL AND resource_schema IN (");
        {
            let mut separated = query_builder.separated(", ");
            for schema in schemas {
                separated.push_bind(schema);
            }
        }
        query_builder.push(")");

        if let Some(exact_names) = exact_names {
            query_builder.push(" AND resource_name COLLATE NOCASE IN (");
            let mut separated = query_builder.separated(", ");
            for name in exact_names {
                separated.push_bind(name.to_string());
            }
            query_builder.push(")");
        }

        if let Some(name_pattern) = name_pattern {
            query_builder.push(" AND resource_name LIKE ");
            query_builder.push_bind(sql_like_escape_pattern(name_pattern));
            query_builder.push(r#" ESCAPE '\' COLLATE NOCASE"#);
        }

        let row = query_builder
            .build_query_scalar::<i64>()
            .fetch_one(connection_mut)
            .await
            .int_err()?;

        usize::try_from(row).int_err()
    }

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let query_id: &uuid::Uuid = query.id.as_ref();
        let maybe_row = sqlx::query!(
            r#"
            SELECT
                resource_id as "id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                resource_schema,
                resource_name,
                description,
                labels as "labels: serde_json::Value",
                annotations as "annotations: serde_json::Value",
                spec as "spec: serde_json::Value",
                status as "status: serde_json::Value",
                generation,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                deleted_at as "deleted_at: DateTime<Utc>",
                last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                last_event_id
            FROM resources
            WHERE resource_id = $1
              AND resource_schema = $2
              AND deleted_at IS NULL
            "#,
            query_id,
            query.schema,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            id: ResourceID::new(row.id),
            schema: row.resource_schema,
            headers: ResourceHeaders {
                account: row.account_id,
                name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                description: row.description,
                labels: serde_json::from_value(row.labels).unwrap(),
                annotations: serde_json::from_value(row.annotations).unwrap(),
                generation: u64::try_from(row.generation).unwrap(),
                created_at: row.created_at,
                updated_at: row.updated_at,
                deleted_at: row.deleted_at,
            },
            spec: row.spec,
            status: row.status,
            last_reconciled_at: row.last_reconciled_at,
            last_event_id: row.last_event_id.map(EventID::new),
        }))
    }

    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &str,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let uids_placeholders =
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_id as id,
                account_id,
                resource_schema,
                resource_name,
                description,
                labels,
                annotations,
                spec,
                status,
                generation,
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            FROM resources
            WHERE resource_schema = $1
              AND resource_id IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(schema);
        for id in ids {
            query = query.bind(*id.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        let mut snapshots_by_id = rows
            .into_iter()
            .map(ResourceSnapshotRow::into_snapshot)
            .map(|snapshot| (snapshot.id, snapshot))
            .collect::<HashMap<_, _>>();

        Ok(ids
            .iter()
            .filter_map(|id| snapshots_by_id.remove(id))
            .collect())
    }

    async fn find_resource_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_id: &uuid::Uuid = id.as_ref();
        let maybe_row = sqlx::query!(
            r#"
            SELECT
                resource_id as "id: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
                resource_schema,
                resource_name,
                description,
                labels as "labels: serde_json::Value",
                annotations as "annotations: serde_json::Value",
                spec as "spec: serde_json::Value",
                status as "status: serde_json::Value",
                generation,
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                deleted_at as "deleted_at: DateTime<Utc>",
                last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                last_event_id
            FROM resources
            WHERE resource_id = $1
              AND deleted_at IS NULL
            "#,
            resource_id,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            id: ResourceID::new(row.id),
            schema: row.resource_schema,
            headers: ResourceHeaders {
                account: row.account_id,
                name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                description: row.description,
                labels: serde_json::from_value(row.labels).unwrap(),
                annotations: serde_json::from_value(row.annotations).unwrap(),
                generation: u64::try_from(row.generation).unwrap(),
                created_at: row.created_at,
                updated_at: row.updated_at,
                deleted_at: row.deleted_at,
            },
            spec: row.spec,
            status: row.status,
            last_reconciled_at: row.last_reconciled_at,
            last_event_id: row.last_event_id.map(EventID::new),
        }))
    }

    async fn find_resource_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let uids_placeholders =
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_id as id,
                account_id,
                resource_schema,
                resource_name,
                description,
                labels,
                annotations,
                spec,
                status,
                generation,
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            FROM resources
            WHERE account_id = $1
              AND resource_id IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(account_id_str);
        for id in ids {
            query = query.bind(*id.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceSnapshot {
                id: ResourceID::new(row.id),
                schema: row.resource_schema,
                headers: ResourceHeaders {
                    account: row.account_id,
                    name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                    description: row.description,
                    labels: serde_json::from_value(row.labels).unwrap(),
                    annotations: serde_json::from_value(row.annotations).unwrap(),
                    generation: u64::try_from(row.generation).unwrap(),
                    created_at: row.created_at,
                    updated_at: row.updated_at,
                    deleted_at: row.deleted_at,
                },
                spec: row.spec,
                status: row.status,
                last_reconciled_at: row.last_reconciled_at,
                last_event_id: row.last_event_id.map(EventID::new),
            })
            .collect())
    }

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        schema: &str,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        let resource_schema = schema.to_owned();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let account_id_stack = account_id.as_stack_string();
            let account_id_str = account_id_stack.as_str();
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT resource_id as "id: uuid::Uuid"
                FROM resources
                WHERE account_id = $1
                  AND resource_schema = $2
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_id DESC
                LIMIT $3 OFFSET $4
                "#,
                account_id_str,
                resource_schema,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(ResourceID::new(row.id));
            }
        })
    }

    fn list_resource_snapshots_by_schema(
        &self,
        account_id: odf::AccountID,
        schema: &str,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let resource_schema = schema.to_owned();

        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let account_id_stack = account_id.as_stack_string();
            let account_id_str = account_id_stack.as_str();
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT
                    resource_id as "id: uuid::Uuid",
                    account_id as "account_id: odf::AccountID",
                    resource_schema,
                    resource_name,
                    description,
                    labels as "labels: serde_json::Value",
                    annotations as "annotations: serde_json::Value",
                    spec as "spec: serde_json::Value",
                    status as "status: serde_json::Value",
                    generation,
                    created_at as "created_at: DateTime<Utc>",
                    updated_at as "updated_at: DateTime<Utc>",
                    deleted_at as "deleted_at: DateTime<Utc>",
                    last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                    last_event_id
                FROM resources
                WHERE account_id = $1
                  AND resource_schema = $2
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_id DESC
                LIMIT $3 OFFSET $4
                "#,
                account_id_str,
                resource_schema,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(ResourceSnapshot {
                    id: ResourceID::new(row.id),
                    schema: row.resource_schema,
                    headers: ResourceHeaders {
                        account: row.account_id,
                        name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                        description: row.description,
                        labels: serde_json::from_value(row.labels).unwrap(),
                        annotations: serde_json::from_value(row.annotations).unwrap(),
                        generation: u64::try_from(row.generation).unwrap(),
                        created_at: row.created_at,
                        updated_at: row.updated_at,
                        deleted_at: row.deleted_at,
                    },
                    spec: row.spec,
                    status: row.status,
                    last_reconciled_at: row.last_reconciled_at,
                    last_event_id: row.last_event_id.map(EventID::new),
                });
            }
        })
    }

    fn list_all_resource_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        Box::pin(async_stream::stream! {
            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let account_id_stack = account_id.as_stack_string();
            let account_id_str = account_id_stack.as_str();
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT
                    resource_id as "id: uuid::Uuid",
                    account_id as "account_id: odf::AccountID",
                    resource_schema,
                    resource_name,
                    description,
                    labels as "labels: serde_json::Value",
                    annotations as "annotations: serde_json::Value",
                    spec as "spec: serde_json::Value",
                    status as "status: serde_json::Value",
                    generation,
                    created_at as "created_at: DateTime<Utc>",
                    updated_at as "updated_at: DateTime<Utc>",
                    deleted_at as "deleted_at: DateTime<Utc>",
                    last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                    last_event_id
                FROM resources
                WHERE account_id = $1
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_id DESC
                LIMIT $2 OFFSET $3
                "#,
                account_id_str,
                limit,
                offset,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(ResourceSnapshot {
                    id: ResourceID::new(row.id),
                    schema: row.resource_schema,
                    headers: ResourceHeaders {
                        account: row.account_id,
                        name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                        description: row.description,
                        labels: serde_json::from_value(row.labels).unwrap(),
                        annotations: serde_json::from_value(row.annotations).unwrap(),
                        generation: u64::try_from(row.generation).unwrap(),
                        created_at: row.created_at,
                        updated_at: row.updated_at,
                        deleted_at: row.deleted_at,
                    },
                    spec: row.spec,
                    status: row.status,
                    last_reconciled_at: row.last_reconciled_at,
                    last_event_id: row.last_event_id.map(EventID::new),
                });
            }
        })
    }

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        schema: &str,
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
              AND resource_schema = $2
              AND deleted_at IS NULL
            "#,
            account_id_str,
            schema,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(count).unwrap())
    }

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let rows = sqlx::query!(
            r#"
            SELECT
                resource_schema,
                COUNT(*) as "total_count!: i64",
                SUM(CASE WHEN phase = 'Reconciling' THEN 1 ELSE 0 END) as "reconciling_count!: i64",
                SUM(CASE WHEN phase = 'Ready' THEN 1 ELSE 0 END) as "ready_count!: i64",
                SUM(CASE WHEN phase = 'Degraded' THEN 1 ELSE 0 END) as "degraded_count!: i64",
                SUM(CASE WHEN phase = 'Failed' THEN 1 ELSE 0 END) as "failed_count!: i64",
                SUM(CASE WHEN phase = 'Pending' THEN 1 ELSE 0 END) as "pending_count!: i64"
            FROM (
                SELECT
                    resource_schema,
                    CASE COALESCE(json_extract(status, '$.phase'), 'Pending')
                        WHEN 'Reconciling' THEN 'Reconciling'
                        WHEN 'Ready' THEN 'Ready'
                        WHEN 'Degraded' THEN 'Degraded'
                        WHEN 'Failed' THEN 'Failed'
                        ELSE 'Pending'
                    END as phase
                FROM resources
                WHERE account_id = $1
                  AND deleted_at IS NULL
            ) as normalized_resources
            GROUP BY resource_schema
            ORDER BY resource_schema ASC
            "#,
            account_id_str,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceSummaryRow {
                schema: row.resource_schema,
                total_count: u64::try_from(row.total_count).unwrap(),
                phase_counts: ResourcePhaseCounts {
                    pending: u64::try_from(row.pending_count).unwrap(),
                    reconciling: u64::try_from(row.reconciling_count).unwrap(),
                    ready: u64::try_from(row.ready_count).unwrap(),
                    degraded: u64::try_from(row.degraded_count).unwrap(),
                    failed: u64::try_from(row.failed_count).unwrap(),
                },
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
