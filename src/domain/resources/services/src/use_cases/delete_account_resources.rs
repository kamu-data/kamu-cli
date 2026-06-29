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
    UnsupportedResourceDescriptorError,
};

use crate::get_resource_crud_dispatcher;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PAGE_SIZE: usize = 100;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
enum DeleteAccountResourceDispatchError {
    #[error(transparent)]
    UnsupportedDescriptor(#[from] UnsupportedResourceDescriptorError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

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
                .list_all_snapshots(account_id.clone(), pagination)
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
            let descriptor_key = (
                resource_snapshot.kind.clone(),
                resource_snapshot.api_version.clone(),
            );

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
            let dispatcher = get_resource_crud_dispatcher::<DeleteAccountResourceDispatchError>(
                &self.catalog,
                &resource_snapshot.kind,
                &resource_snapshot.api_version,
            )
            .map_err(ErrorIntoInternal::int_err)?;

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
