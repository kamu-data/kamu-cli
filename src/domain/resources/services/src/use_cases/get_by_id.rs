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
    TypedResourceQueryService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GetResourceByIdUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
}

impl<'a, R> GetResourceByIdUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    pub fn new(typed_resource_query_service: &'a dyn TypedResourceQueryService<R>) -> Self {
        Self {
            typed_resource_query_service,
        }
    }

    pub async fn execute(
        &self,
        account_id: odf::AccountID,
        id: &ResourceID,
    ) -> Result<R::ResourceState, GetResourceByIdError> {
        self.typed_resource_query_service
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
        #[dill::interface(dyn kamu_resources::GetResourceByIdUseCase<$resource>)]
        pub struct $use_case {
            typed_resource_query_service:
                std::sync::Arc<dyn kamu_resources::TypedResourceQueryService<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::GetResourceByIdUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                id: &kamu_resources::ResourceID,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                kamu_resources::GetResourceByIdError,
            > {
                let helper = $crate::GetResourceByIdUseCaseHelper::<$resource>::new(
                    self.typed_resource_query_service.as_ref(),
                );

                helper.execute(account_id, id).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
