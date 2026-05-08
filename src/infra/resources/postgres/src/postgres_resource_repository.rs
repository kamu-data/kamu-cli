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
    ResourceIdentityRow,
    ResourceMetadata,
    ResourceName,
    ResourcePhaseCounts,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSnapshotRow,
    ResourceSnapshotStream,
    ResourceSnapshotUpdate,
    ResourceSummaryRow,
    ResourceUID,
    ResourceUIDStream,
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
                resource_uid,
                account_id,
                api_version,
                resource_name,
                description,
                labels,
                annotations,
                spec,
                status,
                generation,
                updated_at,
                deleted_at,
                last_reconciled_at,
                last_event_id,
                expected_last_event_id
            ) AS (
            "#,
        );

        query_builder.push_values(resource_updates, |mut b, resource_update| {
            let resource_snapshot = &resource_update.snapshot;

            b.push_bind(*resource_snapshot.uid.as_ref())
                .push_bind(
                    resource_snapshot
                        .metadata
                        .account
                        .as_stack_string()
                        .to_string(),
                )
                .push_bind(resource_snapshot.api_version.clone())
                .push_bind(resource_snapshot.metadata.name.clone())
                .push_bind(resource_snapshot.metadata.description.clone())
                .push_bind(serde_json::to_value(&resource_snapshot.metadata.labels).unwrap())
                .push_bind(serde_json::to_value(&resource_snapshot.metadata.annotations).unwrap())
                .push_bind(resource_snapshot.spec.clone())
                .push_bind(resource_snapshot.status.clone())
                .push_bind(i64::try_from(resource_snapshot.metadata.generation).unwrap())
                .push_bind(resource_snapshot.metadata.updated_at)
                .push_bind(resource_snapshot.metadata.deleted_at)
                .push_bind(resource_snapshot.last_reconciled_at)
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
                    ON r.resource_uid = u.resource_uid
                    AND (
                        r.last_event_id IS NULL AND u.expected_last_event_id IS NULL OR
                        r.last_event_id = u.expected_last_event_id
                    )
            )
            UPDATE resources AS r
            SET
                account_id = u.account_id,
                api_version = u.api_version,
                resource_name = u.resource_name,
                description = u.description,
                labels = u.labels,
                annotations = u.annotations,
                spec = u.spec,
                status = u.status,
                generation = u.generation,
                updated_at = u.updated_at,
                deleted_at = u.deleted_at,
                last_reconciled_at = u.last_reconciled_at,
                last_event_id = u.last_event_id
            FROM matched_resource_updates AS u
            WHERE r.resource_uid = u.resource_uid
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
                        account_id: resource_snapshot.metadata.account.clone(),
                        kind: resource_snapshot.kind.clone(),
                        name: resource_snapshot.metadata.name.clone(),
                    })
                }
                _ => UpdateResourceError::Internal(e.int_err()),
            })?;

        if update_result.rows_affected() != u64::try_from(resource_updates.len()).unwrap() {
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

        let maybe_resource_uid = sqlx::query_scalar!(
            r#"
            SELECT resource_uid as "uid: uuid::Uuid"
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND resource_name = $3
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            kind,
            name.to_ascii_lowercase(),
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
        let uids = uids.iter().map(|uid| *uid.as_ref()).collect::<Vec<_>>();

        let rows = sqlx::query_as!(
            ResourceIdentityRow,
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_uid = ANY($2)
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            &uids,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

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

        let rows = sqlx::query_as!(
            ResourceIdentityRow,
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND resource_name = ANY($3)
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            kind,
            names
                .iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>() as _,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

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
        let limit = i64::try_from(pagination.limit).int_err()?;
        let offset = i64::try_from(pagination.offset).int_err()?;
        let exact_names = exact_names.map(|ns| {
            ns.iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>()
        });
        let name_pattern = name_pattern.map(sql_like_escape_pattern);

        let rows = sqlx::query_as!(
            ResourceIdentityRow,
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
                resource_kind as kind,
                api_version,
                resource_name as name
            FROM resources
            WHERE account_id = $1
              AND resource_kind = ANY($2)
              AND ($3::text[] IS NULL OR resource_name = ANY($3))
              AND ($4::text IS NULL OR resource_name ILIKE $4 ESCAPE '\')
              AND deleted_at IS NULL
            ORDER BY updated_at DESC, resource_uid DESC
            LIMIT $5 OFFSET $6
            "#,
            account_id_stack.as_str(),
            kinds as _,
            exact_names.as_deref() as _,
            name_pattern.as_deref(),
            limit,
            offset,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        Ok(rows)
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
        let exact_names = exact_names.map(|ns| {
            ns.iter()
                .map(|n| n.to_ascii_lowercase())
                .collect::<Vec<_>>()
        });
        let name_pattern = name_pattern.map(sql_like_escape_pattern);

        let row = sqlx::query!(
            r#"
            SELECT COUNT(*) AS count
            FROM resources
            WHERE account_id = $1
              AND resource_kind = ANY($2)
              AND ($3::text[] IS NULL OR resource_name = ANY($3))
              AND ($4::text IS NULL OR resource_name ILIKE $4 ESCAPE '\')
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            kinds as _,
            exact_names.as_deref() as _,
            name_pattern.as_deref(),
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

        let uids = uids.iter().map(|uid| *uid.as_ref()).collect::<Vec<_>>();
        let rows = sqlx::query_as::<_, ResourceSnapshotRow>(
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
              AND resource_uid = ANY($2)
              AND deleted_at IS NULL
            "#,
        )
        .bind(kind)
        .bind(&uids)
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let mut snapshots_by_uid = rows
            .into_iter()
            .map(ResourceSnapshotRow::into_snapshot)
            .map(|snapshot| (snapshot.uid, snapshot))
            .collect::<HashMap<_, _>>();

        Ok(uids
            .into_iter()
            .filter_map(|uid| snapshots_by_uid.remove(&ResourceUID::new(uid)))
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
        let uids = uids.iter().map(|uid| *uid.as_ref()).collect::<Vec<_>>();
        let rows = sqlx::query!(
            r#"
            SELECT
                resource_uid as "uid: uuid::Uuid",
                account_id as "account_id: odf::AccountID",
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
              AND resource_uid = ANY($2)
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
            &uids,
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

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
                account_id_stack.as_str(),
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
                  AND resource_kind = $2
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_uid DESC
                LIMIT $3 OFFSET $4
                "#,
                account_id_stack.as_str(),
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
                  AND deleted_at IS NULL
                ORDER BY updated_at DESC, resource_uid DESC
                LIMIT $2 OFFSET $3
                "#,
                account_id_stack.as_str(),
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

        let count = sqlx::query_scalar!(
            r#"
            SELECT COUNT(*) as "count!"
            FROM resources
            WHERE account_id = $1
              AND resource_kind = $2
              AND deleted_at IS NULL
            "#,
            account_id_stack.as_str(),
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

        let rows = sqlx::query!(
            r#"
            SELECT
                resource_kind,
                api_version,
                COUNT(*) as "total_count!",
                COUNT(*) FILTER (WHERE phase = 'Reconciling') as "reconciling_count!",
                COUNT(*) FILTER (WHERE phase = 'Ready') as "ready_count!",
                COUNT(*) FILTER (WHERE phase = 'Degraded') as "degraded_count!",
                COUNT(*) FILTER (WHERE phase = 'Failed') as "failed_count!",
                COUNT(*) FILTER (WHERE phase = 'Pending') as "pending_count!"
            FROM (
                SELECT
                    resource_kind,
                    api_version,
                    CASE COALESCE(status ->> 'phase', 'Pending')
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
            account_id_stack.as_str(),
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
