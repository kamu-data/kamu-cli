// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_list_resources_by_kind_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ListResourcesByKindUseCase<$resource>)]
        pub struct $use_case {
            resource_query_service:
                std::sync::Arc<dyn $crate::domain::ResourceQueryService<$resource>>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ListResourcesByKindUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                account_id: odf::AccountID,
                pagination: database_common::PaginationOpts,
            ) -> Result<
                Vec<<$resource as $crate::domain::DeclarativeResource>::ResourceState>,
                internal_error::InternalError,
            > {
                self.resource_query_service
                    .list_states_by_kind(account_id, pagination)
                    .await
            }
        }
    };
}

pub(crate) use declare_list_resources_by_kind_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
