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
use kamu_resources::{
    AllResourcesQueryService,
    DeleteAccountResourcesUseCase,
    ResourceID,
    ResourceSnapshot,
    get_resource_deletion_dispatcher_from_catalog,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DeleteAccountResourcesUseCase)]
pub struct DeleteAccountResourcesUsecaseImpl {
    catalog: dill::Catalog,
    all_resources_query_service: Arc<dyn AllResourcesQueryService>,
}

impl DeleteAccountResourcesUsecaseImpl {
    async fn list_owned_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        let mut all_snapshots = Vec::new();
        let mut offset = 0;

        loop {
            let page = self
                .all_resources_query_service
                .list_all_snapshots(
                    account_id.clone(),
                    PaginationOpts {
                        limit: PAGE_SIZE,
                        offset,
                    },
                )
                .await?;

            if page.is_empty() {
                break;
            }

            offset += page.len();
            all_snapshots.extend(page);
        }

        Ok(all_snapshots)
    }

    fn group_resource_ids_by_descriptor(
        &self,
        resource_snapshots: Vec<ResourceSnapshot>,
    ) -> Vec<(ResourceSnapshot, Vec<ResourceID>)> {
        let mut grouped = HashMap::new();

        for resource_snapshot in resource_snapshots {
            let resource_id = resource_snapshot.resource_id;
            let descriptor_key = (
                resource_snapshot.kind.clone(),
                resource_snapshot.api_version.clone(),
            );

            grouped
                .entry(descriptor_key)
                .and_modify(
                    |(_, resource_ids): &mut (ResourceSnapshot, Vec<ResourceID>)| {
                        resource_ids.push(resource_id);
                    },
                )
                .or_insert_with(|| (resource_snapshot, vec![resource_id]));
        }

        grouped.into_values().collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DeleteAccountResourcesUseCase for DeleteAccountResourcesUsecaseImpl {
    async fn execute(&self, account_id: odf::AccountID) -> Result<(), InternalError> {
        let resource_snapshots = self.list_owned_resource_snapshots(&account_id).await?;

        for (resource_snapshot, resource_ids) in
            self.group_resource_ids_by_descriptor(resource_snapshots)
        {
            let dispatcher =
                get_resource_deletion_dispatcher_from_catalog(&self.catalog, &resource_snapshot)?;

            dispatcher
                .delete_resources(&account_id, resource_ids)
                .await?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
