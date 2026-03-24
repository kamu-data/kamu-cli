// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use event_sourcing::EventID;
use internal_error::InternalError;

use crate::{ResourceID, ResourceIDStream, ResourceName, ResourceRawEventQuery};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ResourceRepository: Send + Sync {
    async fn new_resource_id(&self) -> Result<ResourceID, InternalError>;

    async fn get_resource_id_by_name(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError>;

    async fn get_resource_row(
        &self,
        query: &ResourceRawEventQuery,
    ) -> Result<Option<ResourceRow>, InternalError>;

    fn list_resource_ids(
        &self,
        account_id: odf::AccountID,
        kind: &str,
        pagination: PaginationOpts,
    ) -> ResourceIDStream<'_>;

    async fn get_count_resources(
        &self,
        account_id: odf::AccountID,
        kind: &str,
    ) -> Result<usize, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg_attr(feature = "sqlx", derive(sqlx::FromRow))]
#[derive(Debug, Clone)]
pub struct ResourceRow {
    pub resource_id: ResourceID,
    pub account_id: odf::AccountID,
    pub kind: String,
    pub api_version: String,
    pub name: ResourceName,

    pub spec: serde_json::Value,
    pub status: Option<serde_json::Value>,

    pub generation: i64,
    pub observed_generation: Option<i64>,
    pub phase: Option<String>,

    pub created_at: chrono::DateTime<chrono::Utc>,
    pub updated_at: chrono::DateTime<chrono::Utc>,
    pub last_reconciled_at: Option<chrono::DateTime<chrono::Utc>>,
    pub last_event_id: Option<EventID>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
