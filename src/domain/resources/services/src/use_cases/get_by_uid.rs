// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{
    GetResourceByUidError,
    ReconcilableEventSourcedResource,
    ResourceQueryService,
    ResourceUID,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GetResourceByUidUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    resource_query_service: &'a dyn ResourceQueryService<R>,
}

impl<'a, R> GetResourceByUidUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    pub fn new(resource_query_service: &'a dyn ResourceQueryService<R>) -> Self {
        Self {
            resource_query_service,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        uid: &ResourceUID,
    ) -> Result<R::ResourceState, GetResourceByUidError> {
        self.resource_query_service
            .get_state_by_uid(account_id, uid)
            .await
            .map_err(GetResourceByUidError::from)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_get_resource_by_uid_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::GetResourceByUidUseCase<$resource>)]
        pub struct $use_case {
            resource_query_service:
                std::sync::Arc<dyn kamu_resources::ResourceQueryService<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::GetResourceByUidUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                uid: &kamu_resources::ResourceUID,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                kamu_resources::GetResourceByUidError,
            > {
                let helper = $crate::GetResourceByUidUseCaseHelper::<$resource>::new(
                    self.resource_query_service.as_ref(),
                );

                helper.execute(account_id, uid).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
