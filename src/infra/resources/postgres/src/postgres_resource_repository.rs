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
    ResourceDuplicateError,
    ResourceHandleRow,
    ResourceHeaders,
    ResourceID,
    ResourceIDStream,
    ResourceLabelPair,
    ResourceName,
    ResourcePhaseCounts,
    ResourceQuery,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceScope,
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
            -- Request order, matching the in-memory backend: callers pair the
            -- result back to the IDs they asked for. Without this the row order
            -- is whatever the scan happens to produce.
            ORDER BY array_position($2, r.resource_id)
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

        let rows = sqlx::query!(
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.resource_name as name
            FROM resources r
            WHERE r.account_id = $1
              AND r.resource_schema = $2
              AND LOWER(r.resource_name) = ANY($3)
              AND r.deleted_at IS NULL
            -- Request order, matching the in-memory backend: callers pair the
            -- result back to the names they asked for. Without this the row
            -- order is whatever the scan happens to produce.
            ORDER BY array_position($3, LOWER(r.resource_name))
            "#,
            account_id_stack.as_str(),
            schema.as_str(),
            names
                .iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>() as _,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    ResourceName::new_unchecked(&row.name),
                    ResourceID::new(row.id),
                )
            })
            .collect())
    }

    async fn search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceHandleRow>, InternalError> {
        let flat = FlatScope::new(scope, account_id);
        if flat.vacuous {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let limit = i64::try_from(pagination.limit).int_err()?;
        let offset = i64::try_from(pagination.offset).int_err()?;

        sqlx::query_as!(
            ResourceHandleRow,
            r#"
            SELECT
                r.resource_id as "id: uuid::Uuid",
                r.resource_schema as schema,
                r.resource_name as name,
                r.account_id as "account_id: odf::AccountID",
                -- Account row may be missing during async cleanup.
                COALESCE(a.resource_id, '00000000-0000-0000-0000-000000000000'::uuid) as "account_resource_id!: uuid::Uuid",
                COALESCE(a.account_name, $10) as "account_name!"
            FROM resources r
            LEFT JOIN accounts a ON a.id = r.account_id
            WHERE r.deleted_at IS NULL
              -- Each type carries its own query, account and label pairs,
              -- re-paired here. A NULL schema array means "every type", in
              -- which case the single row of query columns applies uniformly.
              -- The account is inside the EXISTS so a row may span an account
              -- of its own.
              AND EXISTS (
                  SELECT 1
                  FROM UNNEST(
                           COALESCE($2::text[], ARRAY[NULL]::text[]),
                           $3::text[],
                           $4::text[],
                           $5::text[],
                           $1::text[]
                       ) WITH ORDINALITY
                         AS f(schema, name_pattern, exact_names, exact_ids, account_id, rn)
                  WHERE r.account_id = f.account_id
                    AND (f.schema IS NULL OR r.resource_schema = f.schema)
                    AND (f.name_pattern IS NULL
                         OR r.resource_name ILIKE f.name_pattern ESCAPE '\')
                    AND (f.exact_names IS NULL
                         OR LOWER(r.resource_name) = ANY(STRING_TO_ARRAY(f.exact_names, ',')))
                    AND (f.exact_ids IS NULL
                         OR r.resource_id = ANY(CAST(STRING_TO_ARRAY(f.exact_ids, ',') AS uuid[])))
                    -- Every label pair belonging to THIS row must be present.
                    -- Pairs are matched to their row by number rather than
                    -- delimiter-joined, since label text is free-form.
                    AND NOT EXISTS (
                        SELECT 1
                        FROM UNNEST($6::int[], $7::text[], $8::text[]) AS l(rn, k, v)
                        WHERE l.rn = f.rn
                          AND NOT EXISTS (
                              SELECT 1
                              FROM resource_labels_projection rl
                              WHERE rl.resource_id = r.resource_id
                                AND rl.label_key = l.k
                                AND rl.label_value = l.v
                          )
                    )
              )
            ORDER BY r.updated_at DESC, r.resource_id DESC
            LIMIT $9 OFFSET $11
            "#,
            flat.account_ids(),
            flat.schemas() as _,
            flat.name_patterns() as _,
            flat.exact_names() as _,
            flat.exact_ids() as _,
            flat.label_row_numbers(),
            flat.label_keys(),
            flat.label_values(),
            limit,
            kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
            offset,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()
    }

    async fn count_search_resource_handles(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
    ) -> Result<usize, InternalError> {
        let flat = FlatScope::new(scope, account_id);
        if flat.vacuous {
            return Ok(0);
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let count = sqlx::query!(
            r#"
            SELECT COUNT(*) AS "count!: i64"
            FROM resources r
            WHERE r.deleted_at IS NULL
              -- Each type carries its own query, account and label pairs,
              -- re-paired here. See `search_resource_handles` for the shape.
              AND EXISTS (
                  SELECT 1
                  FROM UNNEST(
                           COALESCE($2::text[], ARRAY[NULL]::text[]),
                           $3::text[],
                           $4::text[],
                           $5::text[],
                           $1::text[]
                       ) WITH ORDINALITY
                         AS f(schema, name_pattern, exact_names, exact_ids, account_id, rn)
                  WHERE r.account_id = f.account_id
                    AND (f.schema IS NULL OR r.resource_schema = f.schema)
                    AND (f.name_pattern IS NULL
                         OR r.resource_name ILIKE f.name_pattern ESCAPE '\')
                    AND (f.exact_names IS NULL
                         OR LOWER(r.resource_name) = ANY(STRING_TO_ARRAY(f.exact_names, ',')))
                    AND (f.exact_ids IS NULL
                         OR r.resource_id = ANY(CAST(STRING_TO_ARRAY(f.exact_ids, ',') AS uuid[])))
                    AND NOT EXISTS (
                        SELECT 1
                        FROM UNNEST($6::int[], $7::text[], $8::text[]) AS l(rn, k, v)
                        WHERE l.rn = f.rn
                          AND NOT EXISTS (
                              SELECT 1
                              FROM resource_labels_projection rl
                              WHERE rl.resource_id = r.resource_id
                                AND rl.label_key = l.k
                                AND rl.label_value = l.v
                          )
                    )
              )
            "#,
            flat.account_ids(),
            flat.schemas() as _,
            flat.name_patterns() as _,
            flat.exact_names() as _,
            flat.exact_ids() as _,
            flat.label_row_numbers(),
            flat.label_keys(),
            flat.label_values(),
        )
        .fetch_one(connection_mut)
        .await
        .int_err()?
        .count;

        usize::try_from(count).int_err()
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
                -- Account row may be missing during async cleanup.
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

    async fn find_resource_ids_by_schema_and_label(
        &self,
        schema: &TypeUri,
        label_key: &str,
        label_value: &str,
    ) -> Result<Vec<ResourceID>, InternalError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let resource_schema = schema.as_str();

        let rows = sqlx::query!(
            r#"
            SELECT r.resource_id as "id: uuid::Uuid"
            FROM resources r
            JOIN resource_labels_projection rl ON rl.resource_id = r.resource_id
            WHERE r.resource_schema = $1
              AND rl.label_key = $2
              AND rl.label_value = $3
              AND r.deleted_at IS NULL
            ORDER BY r.created_at ASC, r.resource_id ASC
            "#,
            resource_schema,
            label_key,
            label_value,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows.into_iter().map(|row| ResourceID::new(row.id)).collect())
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
            -- Request order, matching the in-memory backend: callers pair the
            -- result back to the IDs they asked for. Without this the row order
            -- is whatever the scan happens to produce.
            ORDER BY array_position($2, r.resource_id)
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

    fn list_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
        scope: &ResourceScope,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_> {
        let flat = FlatScope::new(scope, account_id);

        Box::pin(async_stream::stream! {
            if flat.vacuous {
                return;
            }

            let mut tr = self.transaction.lock().await;
            let connection_mut = tr.connection_mut().await?;

            // The account travels inside `flat`, which owns its strings, so the
            // stream borrows nothing from the caller's `account_id`.
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
                    COALESCE(a.account_name, $10) as "account_name!",
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
                WHERE r.deleted_at IS NULL
                  -- Each type carries its own query, account and label pairs,
                  -- re-paired here. See `search_resource_handles` for the shape.
                  AND EXISTS (
                      SELECT 1
                      FROM UNNEST(
                               COALESCE($2::text[], ARRAY[NULL]::text[]),
                               $3::text[],
                               $4::text[],
                               $5::text[],
                               $1::text[]
                           ) WITH ORDINALITY
                             AS f(schema, name_pattern, exact_names, exact_ids, account_id, rn)
                      WHERE r.account_id = f.account_id
                        AND (f.schema IS NULL OR r.resource_schema = f.schema)
                        AND (f.name_pattern IS NULL
                             OR r.resource_name ILIKE f.name_pattern ESCAPE '\')
                        AND (f.exact_names IS NULL
                             OR LOWER(r.resource_name) = ANY(STRING_TO_ARRAY(f.exact_names, ',')))
                        AND (f.exact_ids IS NULL
                             OR r.resource_id = ANY(CAST(STRING_TO_ARRAY(f.exact_ids, ',') AS uuid[])))
                        AND NOT EXISTS (
                            SELECT 1
                            FROM UNNEST($6::int[], $7::text[], $8::text[]) AS l(rn, k, v)
                            WHERE l.rn = f.rn
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM resource_labels_projection rl
                                  WHERE rl.resource_id = r.resource_id
                                    AND rl.label_key = l.k
                                    AND rl.label_value = l.v
                              )
                        )
                  )
                ORDER BY r.updated_at DESC, r.resource_id DESC
                LIMIT $9 OFFSET $11
                "#,
                flat.account_ids(),
                flat.schemas() as _,
                flat.name_patterns() as _,
                flat.exact_names() as _,
                flat.exact_ids() as _,
                flat.label_row_numbers(),
                flat.label_keys(),
                flat.label_values(),
                limit,
                kamu_resources::DELETED_ACCOUNT_NAME_SENTINEL,
                offset,
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

/// A [`ResourceScope`] flattened into the bind parameters one fixed-arity
/// query needs, so these queries stay inside `sqlx::query_as!` and therefore
/// compile-time checked against the schema.
///
/// Row `i` of the arrays describes one type: `schemas[i]` pairs with
/// `name_patterns[i]`, `exact_names[i]` and `exact_ids[i]`. `UNNEST` re-pairs
/// them inside the SQL.
///
/// The per-row exact lists are encoded as delimited strings because Postgres
/// cannot `UNNEST` a ragged array-of-arrays in parallel with flat columns. The
/// SQL splits them back apart, so each type keeps its own list.
///
/// Label pairs cannot use that trick: names and `UUID`s are guaranteed free of
/// [`Self::LIST_SEPARATOR`], but label keys and values are free-form `TEXT` and
/// may contain anything, so joining them would corrupt a value containing a
/// comma. They are instead kept in three *flat* arrays carrying an explicit row
/// number, unnested separately and re-joined to the scope row by that number.
/// One entry per `(row, pair)`, not per row.
#[derive(Default)]
struct FlatScope {
    /// `NULL` means "no type filter" — see the `$N::text[] IS NULL OR …`
    /// predicate. A single-element row set is used for all-types scopes that
    /// still carry a query.
    schemas: Option<Vec<String>>,
    /// Per row: the `ILIKE` pattern, or `NULL` when that type uses another
    /// mode.
    name_patterns: Vec<Option<String>>,
    /// Per row: exact names joined by [`Self::LIST_SEPARATOR`], already
    /// lowercased, or `NULL`.
    exact_names: Vec<Option<String>>,
    /// Per row: exact IDs joined by [`Self::LIST_SEPARATOR`], or `NULL`.
    exact_ids: Vec<Option<String>>,
    /// Per row: the account that row spans — its own if it named one, else the
    /// call-level default. Never `NULL`, so the predicate needs no `IS NULL`
    /// branch.
    account_ids: Vec<String>,
    /// Which scope row each label pair belongs to, 1-based to match
    /// `WITH ORDINALITY`. Parallel to [`Self::label_keys`] and
    /// [`Self::label_values`].
    label_row_numbers: Vec<i32>,
    label_keys: Vec<String>,
    label_values: Vec<String>,
    /// Set when the scope cannot match anything, letting callers skip the
    /// round-trip entirely.
    vacuous: bool,
}

impl FlatScope {
    /// Neither resource names nor `UUID`s may contain this, so it cannot
    /// collide with the exact-name and exact-id lists it joins.
    ///
    /// That guarantee does **not** extend to label keys and values, which is
    /// why those are passed as flat row-numbered arrays instead.
    const LIST_SEPARATOR: char = ',';

    fn new(scope: &ResourceScope, default_account_id: &odf::AccountID) -> Self {
        let mut flat = Self {
            vacuous: scope.is_vacuous(),
            ..Default::default()
        };

        match scope {
            // `AnyType` carries no per-row account, so the one row takes the
            // call-level default.
            ResourceScope::AnyType(query, label_pairs) => {
                flat.push_row(query.as_ref(), default_account_id.to_string(), label_pairs);
            }
            ResourceScope::Types(types) => {
                let mut schemas = Vec::with_capacity(types.len());
                for entry in types {
                    schemas.push(entry.schema.as_str().to_owned());
                    flat.push_row(
                        entry.query.as_ref(),
                        entry.effective_account_id(default_account_id).to_string(),
                        &entry.label_pairs,
                    );
                }
                flat.schemas = Some(schemas);
            }
        }

        flat
    }

    /// Appends one row's worth of query columns, at most one of which is
    /// non-`NULL`, plus that row's label pairs.
    fn push_row(
        &mut self,
        query: Option<&ResourceQuery>,
        account_id: String,
        label_pairs: &[ResourceLabelPair],
    ) {
        let (name_pattern, exact_names, exact_ids) = match query {
            None => (None, None, None),
            // An ID is matched exactly; only a pattern is `LIKE`-escaped.
            Some(ResourceQuery::NamePattern(pattern)) => {
                (Some(sql_like_escape_pattern(pattern)), None, None)
            }
            Some(ResourceQuery::ExactNames(names)) => (
                None,
                Some(
                    names
                        .iter()
                        .map(|name| name.to_ascii_lowercase())
                        .collect::<Vec<_>>()
                        .join(&Self::LIST_SEPARATOR.to_string()),
                ),
                None,
            ),
            Some(ResourceQuery::ExactIds(ids)) => (
                None,
                None,
                Some(
                    ids.iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(&Self::LIST_SEPARATOR.to_string()),
                ),
            ),
        };

        self.name_patterns.push(name_pattern);
        self.exact_names.push(exact_names);
        self.exact_ids.push(exact_ids);
        self.account_ids.push(account_id);

        // `UNNEST ... WITH ORDINALITY` numbers rows from 1, so this row's index
        // is the count *after* pushing it.
        let row_number = i32::try_from(self.account_ids.len()).unwrap_or(i32::MAX);
        for (key, value) in label_pairs {
            self.label_row_numbers.push(row_number);
            self.label_keys.push(key.to_string());
            self.label_values.push(value.clone());
        }
    }

    fn schemas(&self) -> Option<&[String]> {
        self.schemas.as_deref()
    }

    fn account_ids(&self) -> &[String] {
        &self.account_ids
    }

    fn name_patterns(&self) -> &[Option<String>] {
        &self.name_patterns
    }

    fn exact_names(&self) -> &[Option<String>] {
        &self.exact_names
    }

    fn exact_ids(&self) -> &[Option<String>] {
        &self.exact_ids
    }

    fn label_row_numbers(&self) -> &[i32] {
        &self.label_row_numbers
    }

    fn label_keys(&self) -> &[String] {
        &self.label_keys
    }

    fn label_values(&self) -> &[String] {
        &self.label_values
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
