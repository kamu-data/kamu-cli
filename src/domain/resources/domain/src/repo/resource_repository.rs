// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use event_sourcing::{ConcurrentModificationError, EventID};
use internal_error::InternalError;
use thiserror::Error;

use crate::{
    ResourceID,
    ResourceIDStream,
    ResourceName,
    ResourceRawEventQuery,
    ResourceSnapshot,
    ResourceSnapshotStream,
    ResourceSummaryRow,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceRepository: Send + Sync {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError>;

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError>;

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError>;

    async fn update_resources(
        &self,
        resource_updates: &[ResourceSnapshotUpdate],
    ) -> Result<(), UpdateResourceError> {
        for resource_update in resource_updates {
            self.update_resource(
                &resource_update.snapshot,
                resource_update.expected_last_event_id,
            )
            .await?;
        }

        Ok(())
    }

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    async fn find_resource_identities_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError>;

    async fn count_search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError>;

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn find_resource_snapshots_by_schema_and_ids(
        &self,
        schema: &str,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    async fn find_resource_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn find_resource_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError>;

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        schema: &str,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_>;

    fn list_resource_snapshots_by_schema(
        &self,
        account_id: odf::AccountID,
        schema: &str,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_>;

    fn list_all_resource_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> ResourceSnapshotStream<'_>;

    async fn count_resources(
        &self,
        account_id: odf::AccountID,
        schema: &str,
    ) -> Result<usize, InternalError>;

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateResourceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UpdateResourceError {
    #[error(transparent)]
    Duplicate(ResourceDuplicateError),

    #[error(transparent)]
    ConcurrentModification(ConcurrentModificationError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl UpdateResourceError {
    pub fn concurrent_modification() -> Self {
        Self::ConcurrentModification(ConcurrentModificationError {})
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct ResourceSnapshotUpdate {
    pub snapshot: ResourceSnapshot,
    pub expected_last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
#[error("Resource already exists: account_id={account_id}, schema='{schema}', name='{name}'")]
pub struct ResourceDuplicateError {
    pub account_id: odf::AccountID,
    pub schema: String,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct ResourceIdentityRow {
    pub id: uuid::Uuid,
    pub schema: String,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
pub struct ResourceSnapshotRow {
    pub id: uuid::Uuid,
    pub account_id: odf::AccountID,
    pub resource_schema: String,
    pub resource_name: ResourceName,
    pub description: Option<String>,
    pub labels: serde_json::Value,
    pub annotations: serde_json::Value,
    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,
    pub generation: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
    pub last_reconciled_at: Option<DateTime<Utc>>,
    pub last_event_id: Option<i64>,
}

impl ResourceSnapshotRow {
    pub fn into_snapshot(self) -> ResourceSnapshot {
        ResourceSnapshot {
            id: ResourceID::new(self.id),
            schema: self.resource_schema,
            headers: crate::ResourceHeaders {
                account: self.account_id,
                name: self.resource_name,
                description: self.description,
                labels: serde_json::from_value(self.labels).unwrap(),
                annotations: serde_json::from_value(self.annotations).unwrap(),
                generation: u64::try_from(self.generation).unwrap(),
                created_at: self.created_at,
                updated_at: self.updated_at,
                deleted_at: self.deleted_at,
            },
            spec: self.spec,
            status: self.status,
            last_reconciled_at: self.last_reconciled_at,
            last_event_id: self.last_event_id.map(EventID::new),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
