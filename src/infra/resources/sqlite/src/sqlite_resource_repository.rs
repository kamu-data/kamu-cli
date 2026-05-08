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
    ResourceIdentityRow,
    ResourceMetadata,
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotRow,
    ResourceSnapshotStream,
    ResourceSummaryRow,
    ResourceUID,
    ResourceUIDStream,
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
    async fn new_resource_uid(&self) -> Result<ResourceUID, InternalError> {
        Ok(ResourceUID::new(uuid::Uuid::new_v4()))
    }

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = resource_snapshot.metadata.account.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let labels = serde_json::to_value(&resource_snapshot.metadata.labels).unwrap();
        let annotations = serde_json::to_value(&resource_snapshot.metadata.annotations).unwrap();
        let generation = i64::try_from(resource_snapshot.metadata.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let resource_uid: &uuid::Uuid = resource_snapshot.uid.as_ref();

        sqlx::query!(
            r#"
            INSERT INTO resources (
                resource_uid,
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
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
            resource_uid,
            account_id_str,
            resource_snapshot.kind,
            resource_snapshot.api_version,
            resource_snapshot.metadata.name,
            resource_snapshot.metadata.description,
            labels,
            annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            generation,
            resource_snapshot.metadata.created_at,
            resource_snapshot.metadata.updated_at,
            resource_snapshot.metadata.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.metadata.account.clone(),
                    kind: resource_snapshot.kind.clone(),
                    name: resource_snapshot.metadata.name.clone(),
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

        let account_id_stack = resource_snapshot.metadata.account.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let labels = serde_json::to_value(&resource_snapshot.metadata.labels).unwrap();
        let annotations = serde_json::to_value(&resource_snapshot.metadata.annotations).unwrap();
        let generation = i64::try_from(resource_snapshot.metadata.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let expected_last_event_id = expected_last_event_id.map(EventID::into_inner);
        let resource_uid: &uuid::Uuid = resource_snapshot.uid.as_ref();

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
                updated_at = $11,
                deleted_at = $12,
                last_reconciled_at = $13,
                last_event_id = $14
            WHERE resource_uid = $1
              AND (
                    last_event_id IS NULL AND CAST($15 as INT8) IS NULL OR
                    last_event_id IS NOT NULL AND CAST($15 as INT8) IS NOT NULL AND last_event_id = $15
              )
            "#,
            resource_uid,
            account_id_str,
            resource_snapshot.api_version,
            resource_snapshot.metadata.name,
            resource_snapshot.metadata.description,
            labels,
            annotations,
            resource_snapshot.spec,
            resource_snapshot.status,
            generation,
            resource_snapshot.metadata.updated_at,
            resource_snapshot.metadata.deleted_at,
            resource_snapshot.last_reconciled_at,
            last_event_id,
            expected_last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                UpdateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.metadata.account.clone(),
                    kind: resource_snapshot.kind.clone(),
                    name: resource_snapshot.metadata.name.clone(),
                })
            }
            _ => UpdateResourceError::Internal(e.int_err()),
        })?;

        if update_result.rows_affected() == 0 {
            return Err(UpdateResourceError::concurrent_modification());
        }

        Ok(())
    }

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceUID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let name_lower = name.to_ascii_lowercase();

        let maybe_resource_uid = sqlx::query_scalar!(
            r#"
            SELECT resource_uid as "uid: uuid::Uuid"
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND resource_name = $3
              AND deleted_at IS NULL
            "#,
            account_id_str,
            kind,
            name_lower,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_resource_uid.map(ResourceUID::new))
    }

    async fn find_resource_identities_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let uids_placeholders =
            sqlite_generate_placeholders_list(uids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_uid as uid,
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_uid IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceIdentityRow>(&query_str).bind(account_id_str);
        for uid in uids {
            query = query.bind(*uid.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows)
    }

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
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
                resource_uid as uid,
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND resource_name IN ({names_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceIdentityRow>(&query_str)
            .bind(account_id_str)
            .bind(kind);
        for name in names {
            query = query.bind(name.to_ascii_lowercase());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows)
    }

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        if kinds.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
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
                resource_uid as uid,
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = "#,
        );
        query_builder
            .push_bind(account_id_str)
            .push(" AND deleted_at IS NULL AND resource_kind IN (");
        {
            let mut separated = query_builder.separated(", ");
            for kind in kinds {
                separated.push_bind(kind);
            }
        }
        query_builder.push(")");

        if let Some(exact_names) = exact_names {
            query_builder.push(" AND resource_name IN (");
            let mut separated = query_builder.separated(", ");
            for name in exact_names {
                separated.push_bind(name.to_ascii_lowercase());
            }
            query_builder.push(")");
        }

        if let Some(name_pattern) = name_pattern {
            query_builder.push(" AND resource_name LIKE ");
            query_builder.push_bind(sql_like_escape_pattern(name_pattern));
            query_builder.push(r#" ESCAPE '\' COLLATE NOCASE"#);
        }

        query_builder
            .push(" ORDER BY updated_at DESC, resource_uid DESC LIMIT ")
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
        kinds: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError> {
        if kinds.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
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
            .push(" AND deleted_at IS NULL AND resource_kind IN (");
        {
            let mut separated = query_builder.separated(", ");
            for kind in kinds {
                separated.push_bind(kind);
            }
        }
        query_builder.push(")");

        if let Some(exact_names) = exact_names {
            query_builder.push(" AND resource_name IN (");
            let mut separated = query_builder.separated(", ");
            for name in exact_names {
                separated.push_bind(name.to_ascii_lowercase());
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

        let query_uid: &uuid::Uuid = query.uid.as_ref();
        let maybe_row = sqlx::query!(
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
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
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                deleted_at as "deleted_at: DateTime<Utc>",
                last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                last_event_id
            FROM resources
            WHERE resource_uid = $1
              AND resource_kind = $2
              AND deleted_at IS NULL
            "#,
            query_uid,
            query.kind,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            uid: ResourceUID::new(row.uid),
            kind: row.resource_kind,
            api_version: row.api_version,
            metadata: ResourceMetadata {
                account: row.account_id,
                name: row.resource_name,
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

    async fn find_resource_snapshots_by_kind_and_uids(
        &self,
        kind: &str,
        uids: &[ResourceUID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let uids_placeholders =
            sqlite_generate_placeholders_list(uids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_uid as uid,
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
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            FROM resources
            WHERE resource_kind = $1
              AND resource_uid IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(kind);
        for uid in uids {
            query = query.bind(*uid.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        let mut snapshots_by_uid = rows
            .into_iter()
            .map(ResourceSnapshotRow::into_snapshot)
            .map(|snapshot| (snapshot.uid, snapshot))
            .collect::<HashMap<_, _>>();

        Ok(uids
            .iter()
            .filter_map(|uid| snapshots_by_uid.remove(uid))
            .collect())
    }

    async fn find_resource_snapshot_by_uid(
        &self,
        uid: &ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_uid: &uuid::Uuid = uid.as_ref();
        let maybe_row = sqlx::query!(
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
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
                created_at as "created_at: DateTime<Utc>",
                updated_at as "updated_at: DateTime<Utc>",
                deleted_at as "deleted_at: DateTime<Utc>",
                last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                last_event_id
            FROM resources
            WHERE resource_uid = $1
              AND deleted_at IS NULL
            "#,
            resource_uid,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            uid: ResourceUID::new(row.uid),
            kind: row.resource_kind,
            api_version: row.api_version,
            metadata: ResourceMetadata {
                account: row.account_id,
                name: row.resource_name,
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

    async fn find_resource_snapshots_by_uids(
        &self,
        account_id: &odf::AccountID,
        uids: &[ResourceUID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if uids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let uids_placeholders =
            sqlite_generate_placeholders_list(uids.len(), NonZeroUsize::new(2).unwrap());

        let query_str = format!(
            r#"
            SELECT
                resource_uid as uid,
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
                created_at,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id
            FROM resources
            WHERE account_id = $1
              AND resource_uid IN ({uids_placeholders})
              AND deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(account_id_str);
        for uid in uids {
            query = query.bind(*uid.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceSnapshot {
                uid: ResourceUID::new(row.uid),
                kind: row.resource_kind,
                api_version: row.api_version,
                metadata: ResourceMetadata {
                    account: row.account_id,
                    name: row.resource_name,
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

    fn list_resource_uids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceUIDStream<'_> {
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
                SELECT resource_uid as "uid: uuid::Uuid"
                FROM resources
                WHERE account_id = $1
                  AND resource_kind = $2
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_uid DESC
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
                yield Ok(ResourceUID::new(row.uid));
            }
        })
    }

    fn list_resource_snapshots_by_kind(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
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
                SELECT
                    resource_uid as "uid: uuid::Uuid",
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
                    created_at as "created_at: DateTime<Utc>",
                    updated_at as "updated_at: DateTime<Utc>",
                    deleted_at as "deleted_at: DateTime<Utc>",
                    last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                    last_event_id
                FROM resources
                WHERE account_id = $1
                  AND resource_kind = $2
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_uid DESC
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
                yield Ok(ResourceSnapshot {
                    uid: ResourceUID::new(row.uid),
                    kind: row.resource_kind,
                    api_version: row.api_version,
                    metadata: ResourceMetadata {
                        account: row.account_id,
                        name: row.resource_name,
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
                    resource_uid as "uid: uuid::Uuid",
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
                    created_at as "created_at: DateTime<Utc>",
                    updated_at as "updated_at: DateTime<Utc>",
                    deleted_at as "deleted_at: DateTime<Utc>",
                    last_reconciled_at as "last_reconciled_at: DateTime<Utc>",
                    last_event_id
                FROM resources
                WHERE account_id = $1
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_uid DESC
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
                    uid: ResourceUID::new(row.uid),
                    kind: row.resource_kind,
                    api_version: row.api_version,
                    metadata: ResourceMetadata {
                        account: row.account_id,
                        name: row.resource_name,
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
              AND deleted_at IS NULL
            "#,
            account_id_str,
            kind,
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
                resource_kind,
                api_version,
                COUNT(*) as "total_count!: i64",
                SUM(CASE WHEN phase = 'Reconciling' THEN 1 ELSE 0 END) as "reconciling_count!: i64",
                SUM(CASE WHEN phase = 'Ready' THEN 1 ELSE 0 END) as "ready_count!: i64",
                SUM(CASE WHEN phase = 'Degraded' THEN 1 ELSE 0 END) as "degraded_count!: i64",
                SUM(CASE WHEN phase = 'Failed' THEN 1 ELSE 0 END) as "failed_count!: i64",
                SUM(CASE WHEN phase = 'Pending' THEN 1 ELSE 0 END) as "pending_count!: i64"
            FROM (
                SELECT
                    resource_kind,
                    api_version,
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
            GROUP BY resource_kind, api_version
            ORDER BY resource_kind ASC, api_version ASC
            "#,
            account_id_str,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceSummaryRow {
                kind: row.resource_kind,
                api_version: row.api_version,
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
