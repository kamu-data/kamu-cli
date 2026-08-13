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

use database_common::collect_all_pages;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_resources::{
    DeleteAccountResourcesUseCase,
    GenericResourceQueryService,
    ResourceCrudDispatcherDeleteRequest,
    ResourceID,
    ResourceSnapshot,
};

use crate::get_resource_crud_dispatcher_for_trusted_schema;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DeleteAccountResourcesUseCase)]
pub struct DeleteAccountResourcesUsecaseImpl {
    catalog: dill::Catalog,
    generic_resource_query_service: Arc<dyn GenericResourceQueryService>,
}

impl DeleteAccountResourcesUsecaseImpl {
    async fn list_owned_resource_snapshots(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<Vec<ResourceSnapshot>, InternalError> {
        collect_all_pages(PAGE_SIZE, |pagination| async move {
            self.generic_resource_query_service
                // Account teardown sweeps every resource, never a subset.
                .list_snapshots(
                    account_id,
                    &kamu_resources::ResourceScope::default(),
                    &kamu_resources::ResolvedResourceLabelFilter::True,
                    pagination,
                )
                .await
        })
        .await
    }

    fn group_resource_ids_by_descriptor(
        &self,
        resource_snapshots: Vec<ResourceSnapshot>,
    ) -> Vec<(ResourceSnapshot, Vec<ResourceID>)> {
        let mut grouped = HashMap::new();

        for resource_snapshot in resource_snapshots {
            let id = resource_snapshot.id;
            let descriptor_key = resource_snapshot.schema.clone();

            grouped
                .entry(descriptor_key)
                .and_modify(|(_, ids): &mut (ResourceSnapshot, Vec<ResourceID>)| {
                    ids.push(id);
                })
                .or_insert_with(|| (resource_snapshot, vec![id]));
        }

        grouped.into_values().collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DeleteAccountResourcesUseCase for DeleteAccountResourcesUsecaseImpl {
    async fn execute(&self, account_id: odf::AccountID) -> Result<(), InternalError> {
        let resource_snapshots = self.list_owned_resource_snapshots(&account_id).await?;

        for (resource_snapshot, ids) in self.group_resource_ids_by_descriptor(resource_snapshots) {
            // The schema comes from a stored snapshot, so a missing dispatcher is
            // a data-integrity catastrophe, not a user error.
            let dispatcher = get_resource_crud_dispatcher_for_trusted_schema(
                &self.catalog,
                resource_snapshot.schema.as_str(),
            )?;

            dispatcher
                .delete(ResourceCrudDispatcherDeleteRequest {
                    account_id: account_id.clone(),
                    ids,
                })
                .await
                .map_err(ErrorIntoInternal::int_err)?;
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
