// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
                self.resource_query_service
                    .get_state_by_id(account_id, id)
                    .await
                    .map_err(|err| match err {
                        $crate::domain::TypedResourceQueryError::NotFound(_) => {
                            $crate::domain::GetResourceByIdError::NotFound
                        }
                        err => $crate::domain::GetResourceByIdError::Internal(err.into_internal()),
                    })
            }
        }
    };
}

pub(crate) use declare_get_resource_by_id_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
