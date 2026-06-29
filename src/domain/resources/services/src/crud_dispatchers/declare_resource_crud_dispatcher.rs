// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[macro_export]
macro_rules! declare_resource_crud_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::ResourceCrudDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.schema,
            name: <$resource as kamu_resources::ResourcePresentation>::PRESENTATION.resource_name,
            short_names:
                <$resource as kamu_resources::ResourcePresentation>::PRESENTATION.resource_short_names,
        })]
        pub struct $dispatcher {
            apply_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ApplyResourceUseCase<$resource>>,
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            get_resource_by_id_use_case:
                std::sync::Arc<dyn kamu_resources::GetResourceByIdUseCase<$resource>>,
            list_resources_by_kind_use_case:
                std::sync::Arc<dyn kamu_resources::ListResourcesByKindUseCase<$resource>>,
            delete_resources_use_case:
                std::sync::Arc<dyn kamu_resources::DeleteResourcesUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl $crate::ResourceCrudDispatcher for $dispatcher
        where
            $resource: kamu_resources::ReconcilableEventSourcedResource
                + kamu_resources::ResourceDescriptorProvider
                + kamu_resources::ResourcePresentation,
            <$resource as kamu_resources::DeclarativeResource>::Spec:
                serde::de::DeserializeOwned + serde::Serialize,
            <$resource as kamu_resources::DeclarativeResource>::Status:
                serde::Serialize + kamu_resources::ResourceStatusLike,
            <$resource as kamu_resources::ReconcilableResource>::LifecycleError:
                std::error::Error + Send + Sync + 'static,
        {
            async fn plan_apply(
                &self,
                request: $crate::ResourceCrudDispatcherApplyRequest,
            ) -> Result<
                kamu_resources::ApplyManifestPlanningDecision,
                $crate::ApplyResourceCrudDispatcherError,
            > {
                let schema =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.schema;
                let spec = $crate::decode_resource_spec::<$resource>(schema, request.spec)?;

                let plan = self
                    .apply_resource_use_case
                    .plan(kamu_resources::ApplyResourceParams {
                        id: request.id,
                        headers: request.headers,
                        spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_planning_decision::<$resource>(
                    plan,
                    self.generic_resource_query_service.as_ref(),
                )
                .await
                .map_err(Into::into)
            }

            async fn apply(
                &self,
                request: $crate::ResourceCrudDispatcherApplyRequest,
            ) -> Result<
                kamu_resources::ApplyManifestApplicationDecision,
                $crate::ApplyResourceCrudDispatcherError,
            > {
                let schema =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.schema;
                let spec = $crate::decode_resource_spec::<$resource>(schema, request.spec)?;

                let result = self
                    .apply_resource_use_case
                    .apply(kamu_resources::ApplyResourceParams {
                        id: request.id,
                        headers: request.headers,
                        spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_application_decision::<$resource>(result)
                    .map_err(Into::into)
            }

            async fn get(
                &self,
                request: $crate::ResourceCrudDispatcherGetRequest,
            ) -> Result<kamu_resources::ResourceView, $crate::GetResourceCrudDispatcherError> {
                let state = self
                    .get_resource_by_id_use_case
                    .execute(request.account_id, &request.id)
                    .await
                    .map_err(kamu_resources::GetResourceCrudDispatcherError::from)?;

                $crate::typed_resource_state_to_view::<$resource>(state).map_err(Into::into)
            }

            async fn list(
                &self,
                request: $crate::ResourceCrudDispatcherListRequest,
            ) -> Result<Vec<kamu_resources::ResourceSummaryView>, internal_error::InternalError> {
                let states = self
                    .list_resources_by_kind_use_case
                    .execute(request.account_id, request.pagination)
                    .await?;

                Ok(states
                    .iter()
                    .map($crate::typed_resource_state_to_summary_view::<$resource>)
                    .collect::<Vec<_>>())
            }

            async fn delete(
                &self,
                request: $crate::ResourceCrudDispatcherDeleteRequest,
            ) -> Result<(), $crate::DeleteResourcesCrudDispatcherError> {
                self.delete_resources_use_case
                    .execute(request.account_id, request.ids)
                    .await
                    .map_err(kamu_resources::DeleteResourcesCrudDispatcherError::from)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
