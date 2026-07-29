// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use database_common::{PaginationOpts, TransactionRefT, sql_like_escape_pattern};
use dill::{component, interface};
use event_sourcing::EventID;
use futures::TryStreamExt;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_resources::{
    CreateResourceError,
    ResolvedResourceLabelFilter,
    ResourceDuplicateError,
    ResourceHandleRow,
    ResourceHeaders,
    ResourceID,
    ResourceIDStream,
    ResourceLabelFilterPredicate,
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotRow,
    ResourceSnapshotStream,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    TypeUri,
    UpdateResourceError,
};
use odf::metadata::AsStackString;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResourceRepository)]
pub struct PostgresResourceRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl ResourceRepository for PostgresResourceRepository {
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
            resource_snapshot.schema.as_str(),
            resource_snapshot.headers.name.as_str(),
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
        let resource_update = ResourceSnapshotUpdate {
            snapshot: resource_snapshot.clone(),
            expected_last_event_id,
        };

        self.update_resources(std::slice::from_ref(&resource_update))
            .await
    }

    async fn update_resources(
        &self,
        resource_updates: &[ResourceSnapshotUpdate],
    ) -> Result<(), UpdateResourceError> {
        if resource_updates.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let mut query_builder = sqlx::QueryBuilder::<sqlx::Postgres>::new(
            r#"
            WITH resource_updates(
                resource_id,
                account_id,
                resource_schema,
                resource_name,
                labels,
                annotations,
                spec,
                status,
                generation,
                updated_at,
                deleted_at,
                last_event_id,
                expected_last_event_id
            ) AS (
            "#,
        );

        query_builder.push_values(resource_updates, |mut b, resource_update| {
            let resource_snapshot = &resource_update.snapshot;

            b.push_bind(*resource_snapshot.id.as_ref())
                .push_bind(
                    resource_snapshot
                        .headers
                        .account
                        .did
                        .as_stack_string()
                        .to_string(),
                )
                .push_bind(resource_snapshot.schema.to_string())
                .push_bind(resource_snapshot.headers.name.to_string())
                .push_bind(kamu_resources::resource_labels_to_json(
                    &resource_snapshot.headers.labels,
                ))
                .push_bind(kamu_resources::resource_annotations_to_json(
                    &resource_snapshot.headers.annotations,
                ))
                .push_bind(resource_snapshot.spec.clone())
                .push_bind(
                    resource_snapshot
                        .status
                        .as_ref()
                        .map(kamu_resources::resource_status_to_json),
                )
                .push_bind(i64::try_from(resource_snapshot.headers.generation).unwrap())
                .push_bind(resource_snapshot.headers.updated_at)
                .push_bind(resource_snapshot.headers.deleted_at)
                .push_bind(resource_snapshot.last_event_id.map(EventID::into_inner))
                .push_bind(
                    resource_update
                        .expected_last_event_id
                        .map(EventID::into_inner),
                );
        });

        query_builder.push(
            r#"
            ),
            matched_resource_updates AS (
                SELECT u.*
                FROM resource_updates AS u
                JOIN resources AS r
                    ON r.resource_id = u.resource_id
                    AND (
                        r.last_event_id IS NULL AND u.expected_last_event_id IS NULL OR
                        r.last_event_id = u.expected_last_event_id
                    )
            )
            UPDATE resources AS r
            SET
                account_id = u.account_id,
                resource_schema = u.resource_schema,
                resource_name = u.resource_name,
                labels = u.labels,
                annotations = u.annotations,
                spec = u.spec,
                status = u.status,
                generation = u.generation,
                updated_at = u.updated_at,
                deleted_at = u.deleted_at,
                last_event_id = u.last_event_id
            FROM matched_resource_updates AS u
            WHERE r.resource_id = u.resource_id
              AND (SELECT COUNT(*) FROM matched_resource_updates) =
            "#,
        );
        query_builder.push_bind(i64::try_from(resource_updates.len()).unwrap());

        let update_result = query_builder
            .build()
            .execute(connection_mut)
            .await
            .map_err(|e: sqlx::Error| match e {
                sqlx::Error::Database(e) if e.is_unique_violation() => {
                    let resource_snapshot = &resource_updates[0].snapshot;
                    UpdateResourceError::Duplicate(ResourceDuplicateError {
                        account_id: resource_snapshot.headers.account.did.clone(),
                        schema: resource_snapshot.schema.clone(),
                        name: resource_snapshot.headers.name.clone(),
                    })
                }
                _ => UpdateResourceError::Internal(e.int_err()),
            })?;

        if update_result.rows_affected() != u64::try_from(resource_updates.len()).unwrap() {
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

        let maybe_resource_id = sqlx::query_scalar!(
            r#"
            SELECT resource_id as "id: uuid::Uuid"
            FROM resources
            WHERE account_id = $1
              AND resource_schema = $2
              AND LOWER(resource_name) = LOWER($3)
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            schema.as_str(),
            name.as_str(),
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
        let ids = ids.iter().map(|id| *id.as_ref()).collect::<Vec<_>>();

        let rows = sqlx::query_as!(
            ResourceHandleRow,
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $3) as "account_name!"
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.account_id = $1
              AND r.resource_id = ANY($2)
              AND r.deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            &ids,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn find_resource_handles_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &TypeUri,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        if names.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();

        let rows = sqlx::query_as!(
            ResourceHandleRow,
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $4) as "account_name!"
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.account_id = $1
              AND r.resource_schema = $2
              AND LOWER(r.resource_name) = ANY($3)
              AND r.deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            schema.as_str(),
            names
                .iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>() as _,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        label_filter: &ResolvedResourceLabelFilter,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(Vec::new());
        }

        let (label_keys, label_values) = split_label_filter_pairs(label_filter)?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let schema_strs = schemas.iter().map(TypeUri::as_str).collect::<Vec<_>>();
        let limit = i64::try_from(pagination.limit).int_err()?;
        let offset = i64::try_from(pagination.offset).int_err()?;
        let exact_names = exact_names.map(|ns| {
            ns.iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>()
        });
        let name_pattern = name_pattern.map(sql_like_escape_pattern);

        let rows = sqlx::query_as!(
            ResourceHandleRow,
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $7) as "account_name!"
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.account_id = $1
              AND r.resource_schema = ANY($2)
              AND ($3::text[] IS NULL OR LOWER(r.resource_name) = ANY($3))
              AND ($4::text IS NULL OR r.resource_name ILIKE $4 ESCAPE '\')
              AND r.deleted_at IS NULL
              -- Every authored (key, value) pair must be present. Phrased as
              -- "no pair is missing" so a variable-length filter still fits in
              -- one static query; empty arrays make it vacuously true.
              AND NOT EXISTS (
                  SELECT 1
                  FROM UNNEST($8::text[], $9::text[]) AS f(k, v)
                  WHERE NOT EXISTS (
                      SELECT 1
                      FROM resource_labels_projection rl
                      WHERE rl.resource_id = r.resource_id
                        AND rl.label_key = f.k
                        AND rl.label_value = f.v
                  )
              )
            ORDER BY r.updated_at DESC, r.resource_id DESC
            LIMIT $5 OFFSET $6
            "#,
            account_id_stack.as_str(),
            schema_strs as _,
            exact_names.as_deref() as _,
            name_pattern.as_deref(),
            limit,
            offset,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
            &label_keys,
            &label_values,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows)
    }

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        schemas: &[TypeUri],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        label_filter: &ResolvedResourceLabelFilter,
    ) -> Result<usize, InternalError> {
        if schemas.is_empty() || exact_names.is_some_and(<[ResourceName]>::is_empty) {
            return Ok(0);
        }

        let (label_keys, label_values) = split_label_filter_pairs(label_filter)?;

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let account_id_stack = account_id.as_stack_string();
        let schema_strs = schemas.iter().map(TypeUri::as_str).collect::<Vec<_>>();
        let exact_names = exact_names.map(|ns| {
            ns.iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>()
        });
        let name_pattern = name_pattern.map(sql_like_escape_pattern);

        let row = sqlx::query!(
            r#"
            SELECT COUNT(*) AS count
            FROM resources r
            WHERE r.account_id = $1
              AND r.resource_schema = ANY($2)
              AND ($3::text[] IS NULL OR LOWER(r.resource_name) = ANY($3))
              AND ($4::text IS NULL OR r.resource_name ILIKE $4 ESCAPE '\')
              AND r.deleted_at IS NULL
              -- Same predicate as `search_resource_handles`, so a filtered
              -- count always agrees with its filtered page.
              AND NOT EXISTS (
                  SELECT 1
                  FROM UNNEST($5::text[], $6::text[]) AS f(k, v)
                  WHERE NOT EXISTS (
                      SELECT 1
                      FROM resource_labels_projection rl
                      WHERE rl.resource_id = r.resource_id
                        AND rl.label_key = f.k
                        AND rl.label_value = f.v
                  )
              )
            "#,
            account_id_stack.as_str(),
            schema_strs as _,
            exact_names.as_deref() as _,
            name_pattern.as_deref(),
            &label_keys,
            &label_values,
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?;

        Ok(usize::try_from(row.count.unwrap_or(0)).int_err()?)
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
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $3) as "account_name!",
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

        let ids = ids.iter().map(|id| *id.as_ref()).collect::<Vec<_>>();
        let rows = sqlx::query_as::<_, ResourceSnapshotRow>(
            r#"
            SELECT
                r.resource_id as id,
                r.account_id,
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as account_resource_id,
                COALESCE(a.account_name, $3) as account_name,
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
              AND r.resource_id = ANY($2)
              AND r.deleted_at IS NULL
            "#,
        )
        .bind(schema.as_str())
        .bind(&ids)
        .bind(kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL)
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let mut snapshots_by_id = rows
            .into_iter()
            .map(ResourceSnapshotRow::into_snapshot)
            .map(|snapshot| (snapshot.id, snapshot))
            .collect::<HashMap<_, _>>();

        Ok(ids
            .into_iter()
            .filter_map(|id| snapshots_by_id.remove(&ResourceID::new(id)))
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
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $2) as "account_name!",
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
        let ids = ids.iter().map(|id| *id.as_ref()).collect::<Vec<_>>();
        let rows = sqlx::query!(
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $3) as "account_name!",
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
              AND r.resource_id = ANY($2)
              AND r.deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            &ids,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| ResourceSnapshot {
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
            })
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
                account_id_stack.as_str(),
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
        schema: &TypeUri,
        pagination: PaginationOpts,
        label_filter: &ResolvedResourceLabelFilter,
    ) -> ResourceSnapshotStream<'_> {
        let resource_schema = schema.as_str().to_owned();
        let label_filter = label_filter.clone();

        Box::pin(async_stream::stream! {
            let (label_keys, label_values) = split_label_filter_pairs(&label_filter)?;

            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            let account_id_stack = account_id.as_stack_string();
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT
                    r.resource_id as "id: uuid::Uuid",
                    r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                    COALESCE(a.account_name, $5) as "account_name!",
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
                  AND r.resource_schema = $2
                  AND r.deleted_at IS NULL
                  -- Same predicate as `search_resource_handles`.
                  AND NOT EXISTS (
                      SELECT 1
                      FROM UNNEST($6::text[], $7::text[]) AS f(k, v)
                      WHERE NOT EXISTS (
                          SELECT 1
                          FROM resource_labels_projection rl
                          WHERE rl.resource_id = r.resource_id
                            AND rl.label_key = f.k
                            AND rl.label_value = f.v
                      )
                  )
                ORDER BY r.updated_at DESC, r.resource_id DESC
                LIMIT $3 OFFSET $4
                "#,
                account_id_stack.as_str(),
                resource_schema,
                limit,
                offset,
                kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
                &label_keys,
                &label_values,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(ResourceSnapshot {
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
            let limit = i64::try_from(pagination.limit).int_err()?;
            let offset = i64::try_from(pagination.offset).int_err()?;

            let mut query_stream = sqlx::query!(
                r#"
                SELECT
                    r.resource_id as "id: uuid::Uuid",
                    r.account_id as "account_id: odf::AccountID",
                -- LEFT JOIN: a.resource_id is NULL only when the owning account
                -- row is gone (deletion racing async cleanup), same case the
                -- account_name sentinel covers. Substitute the nil resource id.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                    COALESCE(a.account_name, $4) as "account_name!",
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
                  AND r.deleted_at IS NULL
                ORDER BY r.updated_at DESC, r.resource_id DESC
                LIMIT $2 OFFSET $3
                "#,
                account_id_stack.as_str(),
                limit,
                offset,
                kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
            )
            .fetch(connection_mut)
            .map_err(ErrorIntoInternal::int_err);

            while let Some(row) = query_stream.try_next().await? {
                yield Ok(ResourceSnapshot {
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
                });
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

        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM resources
            WHERE account_id = $1
              AND resource_schema = $2
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            schema.as_str(),
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

        let rows = sqlx::query!(
            r#"
            SELECT
                resource_schema,
                COUNT(*) as "total_count!",
                COUNT(*) FILTER (WHERE status ->> 'phase' = 'Reconciling') as "reconciling_count!",
                COUNT(*) FILTER (WHERE status ->> 'phase' = 'Ready') as "ready_count!",
                COUNT(*) FILTER (WHERE status ->> 'phase' = 'Failed') as "failed_count!",
                COUNT(*) FILTER (
                    WHERE COALESCE(status ->> 'phase', 'Pending') = 'Pending'
                ) as "pending_count!"
            FROM resources
            WHERE account_id = $1
              AND deleted_at IS NULL
            GROUP BY resource_schema
            ORDER BY resource_schema ASC
            "#,
            account_id_stack.as_str(),
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

/// Splits a resolved label filter into the parallel `(keys, values)` arrays
/// bound by the `UNNEST`-based predicates above.
///
/// Keeping the pairs as two `TEXT[]` binds is what lets a variable-length
/// filter live inside a *static* `sqlx::query!`, which is required for
/// compile-time checking against the offline cache. Postgres has `UNNEST`;
/// the `SQLite` backend pushes one `EXISTS` per pair instead.
fn split_label_filter_pairs(
    label_filter: &ResolvedResourceLabelFilter,
) -> Result<(Vec<String>, Vec<String>), InternalError> {
    let pairs = ResourceLabelFilterPredicate::flatten_conjunction(label_filter).int_err()?;

    Ok(pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_owned()))
        .unzip())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
