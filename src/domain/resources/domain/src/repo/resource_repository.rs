// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::{ConcurrentModificationError, EventID};
use internal_error::InternalError;
use thiserror::Error;

use crate::{
    ResourceName,
    ResourceRawEventQuery,
    ResourceSnapshot,
    ResourceSnapshotStream,
    ResourceSummaryRow,
    ResourceUID,
    ResourceUIDStream,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceRepository: Send + Sync {
    async fn new_resource_uid(&self) -> Result<ResourceUID, InternalError>;

    async fn create_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
    ) -> Result<(), CreateResourceError>;

    async fn update_resource(
        &self,
        resource_snapshot: &ResourceSnapshot,
        expected_last_event_id: Option<EventID>,
    ) -> Result<(), UpdateResourceError>;

    async fn find_resource_uid_by_name(
        &self,
        account_id: &odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceUID>, InternalError>;

    async fn find_resource_snapshot(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    async fn find_resource_snapshot_by_uid(
        &self,
        uid: &ResourceUID,
    ) -> Result<Option<ResourceSnapshot>, InternalError>;

    fn list_resource_uids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceUIDStream<'_>;

    fn list_resource_snapshots_by_kind(
        &self,
        account_id: odf::AccountID,
        kind: &str,
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
        kind: &str,
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

#[derive(Error, Debug)]
#[error("Resource already exists: account_id={account_id}, kind='{kind}', name='{name}'")]
pub struct ResourceDuplicateError {
    pub account_id: odf::AccountID,
    pub kind: String,
    pub name: ResourceName,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
