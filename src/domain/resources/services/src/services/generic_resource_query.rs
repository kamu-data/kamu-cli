// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use database_common::PaginationOpts;
use internal_error::InternalError;

use crate::domain::{
    FindOwnedResourceError,
    FindOwnedSnapshotsOutcome,
    GenericResourceQueryService,
    ResourceID,
    ResourceIdentityRow,
    ResourceName,
    ResourceNotOwnedByAccountError,
    ResourceRawEventQuery,
    ResourceRepository,
    ResourceSnapshot,
    ResourceSummaryRow,
    ResourceTypeMismatchError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn GenericResourceQueryService)]
pub struct GenericResourceQueryServiceImpl {
    resource_repository: Arc<dyn ResourceRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl GenericResourceQueryService for GenericResourceQueryServiceImpl {
    async fn allocate_id(&self) -> Result<ResourceID, InternalError> {
        self.resource_repository.new_resource_id().await
    }

    async fn find_resource_id_by_name(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        name: &ResourceName,
    ) -> Result<Option<ResourceID>, InternalError> {
        self.resource_repository
            .find_resource_id_by_name(account_id, schema, name)
            .await
    }

    async fn find_resource_identities_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_ids(account_id, ids)
            .await
    }

    async fn find_resource_identities_by_names(
        &self,
        account_id: &odf::AccountID,
        schema: &str,
        names: &[ResourceName],
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .find_resource_identities_by_names(account_id, schema, names)
            .await
    }

    async fn search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceIdentityRow>, InternalError> {
        self.resource_repository
            .search_resource_identities(account_id, schemas, exact_names, name_pattern, pagination)
            .await
    }

    async fn count_search_resource_identities(
        &self,
        account_id: &odf::AccountID,
        schemas: &[String],
        exact_names: Option<&[ResourceName]>,
        name_pattern: Option<&str>,
    ) -> Result<usize, InternalError> {
        self.resource_repository
            .count_search_resource_identities(account_id, schemas, exact_names, name_pattern)
            .await
    }

    async fn find_owned_snapshot(
        &self,
        account_id: &odf::AccountID,
        schema: &'static str,
        id: ResourceID,
    ) -> Result<Option<ResourceSnapshot>, FindOwnedResourceError> {
        let query = ResourceRawEventQuery {
            schema: schema.to_string(),
            id,
        };

        let Some(resource_snapshot) = self
            .resource_repository
            .find_resource_snapshot(&query)
            .await?
        else {
            return Ok(None);
        };

        if resource_snapshot.headers.account != *account_id {
            return Err(odf::AccessError::Unauthorized(
                ResourceNotOwnedByAccountError {
                    id: resource_snapshot.id,
                    resource_type: schema,
                }
                .into(),
            )
            .into());
        }

        if resource_snapshot.schema != schema {
            return Err(ResourceTypeMismatchError {
                id: resource_snapshot.id,
                expected_schema: schema.to_string(),
                actual_schema: resource_snapshot.schema,
            }
            .into());
        }

        Ok(Some(resource_snapshot))
    }

    async fn find_owned_snapshots(
        &self,
        account_id: &odf::AccountID,
        schema: &'static str,
        ids: &[ResourceID],
    ) -> Result<FindOwnedSnapshotsOutcome, InternalError> {
        let owned_any_kind: HashMap<ResourceID, ResourceSnapshot> = self
            .resource_repository
            .find_resource_snapshots_by_ids(account_id, ids)
            .await?
            .into_iter()
            .map(|snapshot| (snapshot.id, snapshot))
            .collect();

        let schema_mismatch: Vec<(ResourceID, String)> = owned_any_kind
            .values()
            .filter(|snapshot| snapshot.schema != schema)
            .map(|snapshot| (snapshot.id, snapshot.schema.clone()))
            .collect();

        let missing_ids: Vec<ResourceID> = ids
            .iter()
            .copied()
            .filter(|id| !owned_any_kind.contains_key(id))
            .collect();

        let access_denied_snapshots = self
            .resource_repository
            .find_resource_snapshots_by_schema_and_ids(schema, &missing_ids)
            .await?;
        let access_denied_ids: std::collections::HashSet<ResourceID> =
            access_denied_snapshots.iter().map(|s| s.id).collect();

        let access_denied: Vec<ResourceID> = access_denied_ids.iter().copied().collect();

        let not_found: Vec<ResourceID> = missing_ids
            .into_iter()
            .filter(|id| !access_denied_ids.contains(id))
            .collect();

        let found: Vec<ResourceSnapshot> = owned_any_kind
            .into_values()
            .filter(|snapshot| snapshot.schema == schema)
            .collect();

        Ok(FindOwnedSnapshotsOutcome {
            found,
            not_found,
            schema_mismatch,
            access_denied,
        })
    }

    async fn get_snapshot_by_id(
        &self,
        id: &ResourceID,
    ) -> Result<Option<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshot_by_id(id)
            .await
    }

    async fn find_snapshots_by_ids(
        &self,
        account_id: &odf::AccountID,
        ids: &[ResourceID],
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        self.resource_repository
            .find_resource_snapshots_by_ids(account_id, ids)
            .await
    }

    async fn list_snapshots_by_schema(
        &self,
        account_id: odf::AccountID,
        schema: &str,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_resource_snapshots_by_schema(account_id, schema, pagination);

        use tokio_stream::StreamExt;

        let mut resource_snapshots = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            resource_snapshots.push(resource_snapshot?);
        }

        Ok(resource_snapshots)
    }

    async fn list_all_snapshots(
        &self,
        account_id: odf::AccountID,
        pagination: PaginationOpts,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let mut resource_snapshots_stream = self
            .resource_repository
            .list_all_resource_snapshots(account_id, pagination);

        use tokio_stream::StreamExt;

        let mut resource_snapshots = Vec::new();
        while let Some(resource_snapshot) = resource_snapshots_stream.next().await {
            resource_snapshots.push(resource_snapshot?);
        }

        Ok(resource_snapshots)
    }

    async fn summarize_resources(
        &self,
        account_id: odf::AccountID,
    ) -> Result<Vec<ResourceSummaryRow>, InternalError> {
        self.resource_repository
            .summarize_resources(account_id)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
