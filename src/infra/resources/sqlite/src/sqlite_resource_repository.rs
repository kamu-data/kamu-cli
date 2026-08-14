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
    DELETED_ACCOUNT_NAME_SENTINEL,
    ResolvedResourceLabelFilter,
    ResourceDuplicateError,
    ResourceHandleRow,
    ResourceHeaders,
    ResourceID,
    ResourceIDStream,
    ResourceLabelFilterPredicate,
    ResourceName,
    ResourcePhaseCounts,
    ResourceQuery,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceScope,
    ResourceSnapshot,
    ResourceSnapshotRow,
    ResourceSnapshotStream,
    ResourceSummaryRow,
    TypeRef,
    TypeUri,
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

        let account_id_stack = resource_snapshot.headers.account.did.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let resource_snapshot_schema = resource_snapshot.schema.as_str();
        let name_str = resource_snapshot.headers.name.as_str();
        let labels = kamu_resources::resource_labels_to_json(&resource_snapshot.headers.labels);
        let annotations =
            kamu_resources::resource_annotations_to_json(&resource_snapshot.headers.annotations);
        let generation = i64::try_from(resource_snapshot.headers.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let resource_id: &uuid::Uuid = resource_snapshot.id.as_ref();
        let status = resource_snapshot
            .status
            .as_ref()
            .map(kamu_resources::resource_status_to_json);

        sqlx::query!(
            r#"
            INSERT INTO resources (
                resource_id,
                account_id,
                resource_schema,
                resource_name,
                labels,
                annotations,
                spec,
                status,
                generation,
                created_at,
                updated_at,
                deleted_at,
                last_event_id
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
            resource_id,
            account_id_str,
            resource_snapshot_schema,
            name_str,
            labels,
            annotations,
            resource_snapshot.spec,
            status,
            generation,
            resource_snapshot.headers.created_at,
            resource_snapshot.headers.updated_at,
            resource_snapshot.headers.deleted_at,
            last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                CreateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.headers.account.did.clone(),
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

        let account_id_stack = resource_snapshot.headers.account.did.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let resource_snapshot_schema = resource_snapshot.schema.as_str();
        let name_str = resource_snapshot.headers.name.as_str();
        let labels = kamu_resources::resource_labels_to_json(&resource_snapshot.headers.labels);
        let annotations =
            kamu_resources::resource_annotations_to_json(&resource_snapshot.headers.annotations);
        let generation = i64::try_from(resource_snapshot.headers.generation).unwrap();
        let last_event_id = resource_snapshot.last_event_id.map(EventID::into_inner);
        let expected_last_event_id = expected_last_event_id.map(EventID::into_inner);
        let resource_id: &uuid::Uuid = resource_snapshot.id.as_ref();
        let status = resource_snapshot
            .status
            .as_ref()
            .map(kamu_resources::resource_status_to_json);

        let update_result = sqlx::query!(
            r#"
            UPDATE resources
            SET
                account_id = $2,
                resource_schema = $3,
                resource_name = $4,
                labels = $5,
                annotations = $6,
                spec = $7,
                status = $8,
                generation = $9,
                updated_at = $10,
                deleted_at = $11,
                last_event_id = $12
            WHERE resource_id = $1
              AND (
                    last_event_id IS NULL AND CAST($13 as INT8) IS NULL OR
                    last_event_id IS NOT NULL AND CAST($13 as INT8) IS NOT NULL AND last_event_id = $13
              )
            "#,
            resource_id,
            account_id_str,
            resource_snapshot_schema,
            name_str,
            labels,
            annotations,
            resource_snapshot.spec,
            status,
            generation,
            resource_snapshot.headers.updated_at,
            resource_snapshot.headers.deleted_at,
            last_event_id,
            expected_last_event_id,
        )
        .execute(connection_mut)
        .await
        .map_err(|e: sqlx::Error| match e {
            sqlx::Error::Database(e) if e.is_unique_violation() => {
                UpdateResourceError::Duplicate(ResourceDuplicateError {
                    account_id: resource_snapshot.headers.account.did.clone(),
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
        schema: &TypeUri,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let schema = schema.as_str();
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

    async fn find_resource_handles_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();

        let uids_placeholders =
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(3).unwrap());

        let query_str = format!(
            r#"
            SELECT
                r.resource_id as id,
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as account_id,
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as account_resource_id,
                COALESCE(a.account_name, $2) as account_name
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.account_id = $1
              AND r.resource_id IN ({uids_placeholders})
              AND r.deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceHandleRow>(&query_str)
            .bind(account_id_str)
            .bind(kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL);
        for id in ids {
            query = query.bind(*id.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows)
    }

    async fn resolve_resource_ids_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
    ) -> Result<Vec<(ResourceName, ResourceID)>, InternalError> {
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
                r.resource_id as id,
                r.resource_name as name
            FROM resources r
            WHERE r.account_id = $1
              AND r.resource_schema = $2
              AND r.resource_name COLLATE NOCASE IN ({names_placeholders})
              AND r.deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, (uuid::Uuid, String)>(&query_str)
            .bind(account_id_str)
            .bind(schema.as_str());

        for name in names {
            query = query.bind(name.to_string());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows
            .into_iter()
            .map(|(id, name)| (ResourceName::new_unchecked(&name), ResourceID::new(id)))
            .collect())
    }

    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        if scope.is_vacuous() {
            return Ok(Vec::new());
        }

        let label_pairs =
            ResourceLabelFilterPredicate::flatten_conjunction(label_filter).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let limit = i64::try_from(pagination.limit).int_err()?;
        let offset = i64::try_from(pagination.offset).int_err()?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT
                r.resource_id as id,
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as account_id,
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as account_resource_id,
                COALESCE(a.account_name, "#,
        );
        query_builder.push_bind(kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL);
        query_builder.push(
            r#") as account_name
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.deleted_at IS NULL"#,
        );

        push_scope_predicate(&mut query_builder, scope, account_id);
        push_label_filter_predicates(&mut query_builder, &label_pairs);

        query_builder
            .push(" ORDER BY r.updated_at DESC, r.resource_id DESC LIMIT ")
            .push_bind(limit)
            .push(" OFFSET ")
            .push_bind(offset);

        query_builder
            .build_query_as::<ResourceHandleRow>()
            .fetch_all(connection_mut)
            .await
            .int_err()
    }

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        label_filter: &ResolvedResourceLabelFilter,
    ) -> Result<usize, InternalError> {
        if scope.is_vacuous() {
            return Ok(0);
        }

        let label_pairs =
            ResourceLabelFilterPredicate::flatten_conjunction(label_filter).int_err()?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
            r#"
            SELECT COUNT(*) as count
            FROM resources r
            WHERE r.deleted_at IS NULL"#,
        );

        push_scope_predicate(&mut query_builder, scope, account_id);
        push_label_filter_predicates(&mut query_builder, &label_pairs);

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
        let query_schema = query.schema.as_str();
        let maybe_row = sqlx::query!(
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.account_id as "account_id: odf::AccountID",
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $3) as "account_name!: String",
                r.resource_schema,
                r.resource_name,
                r.labels as "labels: serde_json::Value",
                r.annotations as "annotations: serde_json::Value",
                r.spec as "spec: serde_json::Value",
                r.status as "status: serde_json::Value",
                r.generation,
                r.created_at as "created_at: DateTime<Utc>",
                r.updated_at as "updated_at: DateTime<Utc>",
                r.deleted_at as "deleted_at: DateTime<Utc>",
                r.last_event_id
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.resource_id = $1
              AND r.resource_schema = $2
              AND r.deleted_at IS NULL
            "#,
            query_id,
            query_schema,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            id: ResourceID::new(row.id),
            schema: TypeUri::new_unchecked(row.resource_schema),
            headers: ResourceHeaders {
                id: ResourceID::new(row.id),
                account: odf::AccountHandle {
                    id: ResourceID::new(row.account_resource_id),
                    did: row.account_id,
                    name: odf::AccountName::new_unchecked(&row.account_name),
                },
                name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                labels: kamu_resources::resource_labels_from_json(row.labels),
                annotations: kamu_resources::resource_annotations_from_json(row.annotations),
                generation: u64::try_from(row.generation).unwrap(),
                created_at: row.created_at,
                updated_at: row.updated_at,
                deleted_at: row.deleted_at,
            },
            spec: row.spec,
            status: row
                .status
                .as_ref()
                .and_then(ResourceSnapshot::basic_status_from_json),
            last_event_id: row.last_event_id.map(EventID::new),
        }))
    }

    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &TypeUri,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let uids_placeholders =
            sqlite_generate_placeholders_list(ids.len(), NonZeroUsize::new(2).unwrap());
        let deleted_account_sentinel = kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL;

        let query_str = format!(
            r#"
            SELECT
                r.resource_id as id,
                r.account_id,
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as account_resource_id,
                COALESCE(a.account_name, '{deleted_account_sentinel}') as account_name,
                r.resource_schema,
                r.resource_name,
                r.labels,
                r.annotations,
                r.spec,
                r.status,
                r.generation,
                r.created_at,
                r.updated_at,
                r.deleted_at,
                r.last_event_id
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.resource_schema = $1
              AND r.resource_id IN ({uids_placeholders})
              AND r.deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(schema.as_str());
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
                r.resource_id as "id: uuid::Uuid",
                r.account_id as "account_id: odf::AccountID",
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $2) as "account_name!: String",
                r.resource_schema,
                r.resource_name,
                r.labels as "labels: serde_json::Value",
                r.annotations as "annotations: serde_json::Value",
                r.spec as "spec: serde_json::Value",
                r.status as "status: serde_json::Value",
                r.generation,
                r.created_at as "created_at: DateTime<Utc>",
                r.updated_at as "updated_at: DateTime<Utc>",
                r.deleted_at as "deleted_at: DateTime<Utc>",
                r.last_event_id
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.resource_id = $1
              AND r.deleted_at IS NULL
            "#,
            resource_id,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        Ok(maybe_row.map(|row| ResourceSnapshot {
            id: ResourceID::new(row.id),
            schema: TypeUri::new_unchecked(row.resource_schema),
            headers: ResourceHeaders {
                id: ResourceID::new(row.id),
                account: odf::AccountHandle {
                    id: ResourceID::new(row.account_resource_id),
                    did: row.account_id,
                    name: odf::AccountName::new_unchecked(&row.account_name),
                },
                name: kamu_resources::ResourceName::new_unchecked(&row.resource_name),
                labels: kamu_resources::resource_labels_from_json(row.labels),
                annotations: kamu_resources::resource_annotations_from_json(row.annotations),
                generation: u64::try_from(row.generation).unwrap(),
                created_at: row.created_at,
                updated_at: row.updated_at,
                deleted_at: row.deleted_at,
            },
            spec: row.spec,
            status: row
                .status
                .as_ref()
                .and_then(ResourceSnapshot::basic_status_from_json),
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
        let deleted_account_sentinel = kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL;

        let query_str = format!(
            r#"
            SELECT
                r.resource_id as id,
                r.account_id,
                COALESCE(a.resource_id, X'00000000000000000000000000000000') as account_resource_id,
                COALESCE(a.account_name, '{deleted_account_sentinel}') as account_name,
                r.resource_schema,
                r.resource_name,
                r.labels,
                r.annotations,
                r.spec,
                r.status,
                r.generation,
                r.created_at,
                r.updated_at,
                r.deleted_at,
                r.last_event_id
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.account_id = $1
              AND r.resource_id IN ({uids_placeholders})
              AND r.deleted_at IS NULL
            "#,
        );

        let mut query = sqlx::query_as::<_, ResourceSnapshotRow>(&query_str).bind(account_id_str);
        for id in ids {
            query = query.bind(*id.as_ref());
        }

        let rows = query.fetch_all(connection_mut).await.int_err()?;

        Ok(rows
            .into_iter()
            .map(ResourceSnapshotRow::into_snapshot)
            .collect())
    }

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_> {
        let resource_schema = schema.as_str().to_owned();

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

    fn list_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let account_id = account_id.clone();
        let label_filter = label_filter.clone();
        let scope = scope.clone();

        Box::pin(async_stream::stream! {
            let label_pairs =
                ResourceLabelFilterPredicate::flatten_conjunction(&label_filter).int_err()?;

            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_builder = sqlx::QueryBuilder::<sqlx::Sqlite>::new(
                r#"
                SELECT
                    r.resource_id as id,
                    r.account_id,
                    COALESCE(a.resource_id, X'00000000000000000000000000000000') as account_resource_id,
                    COALESCE(a.account_name, "#,
            );
            query_builder.push_bind(DELETED_ACCOUNT_NAME_SENTINEL);
            query_builder.push(
                r#") as account_name,
                    r.resource_schema,
                    r.resource_name,
                    r.labels,
                    r.annotations,
                    r.spec,
                    r.status,
                    r.generation,
                    r.created_at,
                    r.updated_at,
                    r.deleted_at,
                    r.last_event_id
                FROM resources r
                LEFT JOIN accounts a ON a.id = r.account_id
                WHERE r.deleted_at IS NULL"#,
            );

            push_scope_predicate(&mut query_builder, &scope, &account_id);
            push_label_filter_predicates(&mut query_builder, &label_pairs);

            query_builder
                .push(" ORDER BY r.updated_at DESC, r.resource_id DESC LIMIT ")
                .push_bind(limit)
                .push(" OFFSET ")
                .push_bind(offset);

            let mut query_stream = query_builder
                .build_query_as::<ResourceSnapshotRow>()
                .fetch(connection_mut)
                .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(row.into_snapshot());
            }
        })
    }

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        schema: &TypeUri,
    ) -> Result<usize, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let account_id_str = account_id_stack.as_str();
        let schema = schema.as_str();

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
                SUM(
                    CASE WHEN json_extract(status, '$.phase') = 'Reconciling' THEN 1 ELSE 0 END
                ) as "reconciling_count!: i64",
                SUM(
                    CASE WHEN json_extract(status, '$.phase') = 'Ready' THEN 1 ELSE 0 END
                ) as "ready_count!: i64",
                SUM(
                    CASE WHEN json_extract(status, '$.phase') = 'Failed' THEN 1 ELSE 0 END
                ) as "failed_count!: i64",
                SUM(
                    CASE
                        WHEN COALESCE(json_extract(status, '$.phase'), 'Pending') = 'Pending'
                        THEN 1
                        ELSE 0
                    END
                ) as "pending_count!: i64"
            FROM resources
            WHERE account_id = $1
              AND deleted_at IS NULL
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
                    failed: u64::try_from(row.failed_count).unwrap(),
                },
            })
            .collect())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends the bare predicate matching `query`'s one active mode, with no
/// leading connective so it can be combined either way.
fn push_query_predicate(
    query_builder: &mut sqlx::QueryBuilder<'_, sqlx::Sqlite>,
    query: &ResourceQuery,
) {
    match query {
        ResourceQuery::ExactNames(names) => {
            query_builder.push("r.resource_name COLLATE NOCASE IN (");
            let mut separated = query_builder.separated(", ");
            for name in names {
                separated.push_bind(name.to_string());
            }
            query_builder.push(")");
        }
        ResourceQuery::ExactIds(ids) => {
            query_builder.push("r.resource_id IN (");
            let mut separated = query_builder.separated(", ");
            for id in ids {
                separated.push_bind(*id.as_ref());
            }
            query_builder.push(")");
        }
        // An ID is matched exactly above; only a pattern is LIKE-escaped.
        ResourceQuery::NamePattern(pattern) => {
            query_builder.push("r.resource_name LIKE ");
            query_builder.push_bind(sql_like_escape_pattern(pattern));
            query_builder.push(r#" ESCAPE '\' COLLATE NOCASE"#);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends the scope predicate: one `OR` group per type, each pairing the
/// schema with that type's own query.
/// Appends the scope predicate, **including the account**.
///
/// The account lives inside each OR-group rather than as one outer
/// `r.account_id = ?`, because a row may name its own account; rows that do not
/// use `default_account_id`. `AnyType` carries no per-row account, so it keeps
/// the single outer term.
fn push_scope_predicate(
    query_builder: &mut sqlx::QueryBuilder<'_, sqlx::Sqlite>,
    scope: &ResourceScope,
    default_account_id: &odf::AccountID,
) {
    let push_account = |query_builder: &mut sqlx::QueryBuilder<'_, sqlx::Sqlite>,
                        account_id: &odf::AccountID| {
        query_builder.push("r.account_id = ");
        query_builder.push_bind(account_id.to_string());
    };

    match scope {
        ResourceScope::AnyType(query) => {
            query_builder.push(" AND ");
            push_account(query_builder, default_account_id);
            if let Some(query) = query {
                query_builder.push(" AND ");
                push_query_predicate(query_builder, query);
            }
        }
        ResourceScope::Types(types) => {
            // An empty scope matches nothing; without this the `OR` chain would
            // collapse to an always-true `AND ()`.
            if types.is_empty() {
                query_builder.push(" AND 1 = 0");
                return;
            }

            query_builder.push(" AND (");
            for (index, entry) in types.iter().enumerate() {
                if index > 0 {
                    query_builder.push(" OR ");
                }
                query_builder.push("(");
                push_account(
                    query_builder,
                    entry.effective_account_id(default_account_id),
                );
                query_builder.push(" AND r.resource_schema = ");
                query_builder.push_bind(entry.schema.as_str().to_owned());
                if let Some(query) = &entry.query {
                    query_builder.push(" AND ");
                    push_query_predicate(query_builder, query);
                }
                query_builder.push(")");
            }
            query_builder.push(")");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Appends one label-projection predicate per `(key, value)` pair.
fn push_label_filter_predicates(
    query_builder: &mut sqlx::QueryBuilder<'_, sqlx::Sqlite>,
    label_pairs: &[(&TypeRef, &str)],
) {
    for (key, value) in label_pairs {
        query_builder.push(
            " AND EXISTS (SELECT 1 FROM resource_labels_projection rl WHERE rl.resource_id = \
             r.resource_id AND rl.label_key = ",
        );
        query_builder.push_bind(key.to_string());
        query_builder.push(" AND rl.label_value = ");
        query_builder.push_bind((*value).to_string());
        query_builder.push(")");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
