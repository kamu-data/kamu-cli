// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{
    GetResourceByIdError,
    ReconcilableEventSourcedResource,
    ResourceID,
    ResourceQueryService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GetResourceByIdUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    resource_query_service: &'a dyn ResourceQueryService<R>,
}

impl<'a, R> GetResourceByIdUseCaseHelper<'a, R>
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
        id: &ResourceID,
    ) -> Result<R::ResourceState, GetResourceByIdError> {
        self.resource_query_service
            .get_state_by_id(account_id, id)
            .await
            .map_err(GetResourceByIdError::from)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_get_resource_by_id_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::GetResourceByIdUseCase<$resource>)]
        pub struct $use_case {
            resource_query_service:
                std::sync::Arc<dyn $crate::domain::ResourceQueryService<$resource>>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::GetResourceByIdUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                id: &$crate::domain::ResourceID,
            ) -> Result<
                <$resource as $crate::domain::DeclarativeResource>::ResourceState,
                $crate::domain::GetResourceByIdError,
            > {
                let helper = $crate::GetResourceByIdUseCaseHelper::<$resource>::new(
                    self.resource_query_service.as_ref(),
                );

                helper.execute(account_id, id).await
            }
        }
    };
}

pub use declare_get_resource_by_id_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
