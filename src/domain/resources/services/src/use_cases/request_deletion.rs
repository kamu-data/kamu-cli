// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashSet;

use crate::domain::{
    EventSourcedResource,
    GenericResourceQueryService,
    RequestResourcesDeletionError,
    ResourceID,
    ResourceLifecyclePrimitives,
    ResourceNotOwnedByAccountError,
    ResourceSchemaProvider,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RequestResourcesDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
{
    generic_resource_query_service: &'a dyn GenericResourceQueryService,
    lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
}

impl<'a, R> RequestResourcesDeletionUseCaseHelper<'a, R>
where
    R: EventSourcedResource + ResourceSchemaProvider,
    R::LifecycleError: std::fmt::Display,
{
    pub fn new(
        generic_resource_query_service: &'a dyn GenericResourceQueryService,
        lifecycle_primitives: &'a dyn ResourceLifecyclePrimitives<R>,
    ) -> Self {
        Self {
            generic_resource_query_service,
            lifecycle_primitives,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        ids: Vec<ResourceID>,
    ) -> Result<(), RequestResourcesDeletionError> {
        let unique_ids = ids
            .into_iter()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();

        let outcome = self
            .generic_resource_query_service
            .find_owned_snapshots(&account_id, R::schema(), &unique_ids)
            .await
            .map_err(RequestResourcesDeletionError::Internal)?;

        if let Some(&id) = outcome.access_denied.first() {
            return Err(RequestResourcesDeletionError::Access(
                odf::AccessError::Unauthorized(
                    ResourceNotOwnedByAccountError {
                        id,
                        resource_type: R::schema(),
                    }
                    .into(),
                ),
            ));
        }

        let owned_resource_ids = outcome
            .found
            .into_iter()
            .map(|snapshot| snapshot.id)
            .collect::<Vec<_>>();

        self.lifecycle_primitives
            .request_deletion(&owned_resource_ids)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_request_resources_deletion_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::RequestResourcesDeletionUseCase<$resource>)]
        pub struct $use_case {
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            lifecycle_primitives:
                std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::RequestResourcesDeletionUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                ids: Vec<kamu_resources::ResourceID>,
            ) -> Result<(), kamu_resources::RequestResourcesDeletionError> {
                let helper = $crate::RequestResourcesDeletionUseCaseHelper::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.lifecycle_primitives.as_ref(),
                );

                helper.execute(account_id, ids).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
