// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;

use crate::domain::{ReconcilableEventSourcedResource, TypedResourceQueryService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ListResourcesByKindUseCaseHelper<'a, R>
where
    R: ReconcilableEventSourcedResource,
{
    typed_resource_query_service: &'a dyn TypedResourceQueryService<R>,
}

impl<'a, R> ListResourcesByKindUseCaseHelper<'a, R>
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
        pagination: PaginationOpts,
    ) -> Result<Vec<R::ResourceState>, internal_error::InternalError> {
        self.typed_resource_query_service
            .list_states_by_kind(account_id, pagination)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_list_resources_by_kind_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ListResourcesByKindUseCase<$resource>)]
        pub struct $use_case {
            typed_resource_query_service:
                std::sync::Arc<dyn kamu_resources::TypedResourceQueryService<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ListResourcesByKindUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as kamu_resources::DeclarativeResource>::ResourceState>,
                internal_error::InternalError,
            > {
                let helper = $crate::ListResourcesByKindUseCaseHelper::<$resource>::new(
                    self.typed_resource_query_service.as_ref(),
                );

                helper.execute(account_id, pagination).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
