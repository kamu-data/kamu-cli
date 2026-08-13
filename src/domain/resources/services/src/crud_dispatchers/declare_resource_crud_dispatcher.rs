// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[rustfmt::skip]
#[macro_export]
macro_rules! declare_resource_crud_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::ResourceCrudDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource>::SCHEMA_STR,
            canonical_selector: <$resource>::CANONICAL_SELECTOR_NAME_STR,
            selector_aliases: <$resource>::SELECTOR_ALIAS_STRS,
        })]
        pub struct $dispatcher {
            apply_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ApplyResourceUseCase<$resource>>,
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            get_resource_by_id_use_case:
                std::sync::Arc<dyn kamu_resources::GetResourceByIdUseCase<$resource>>,
            list_resources_by_type_use_case:
                std::sync::Arc<dyn kamu_resources::ListResourcesByTypeUseCase<$resource>>,
            delete_resources_use_case:
                std::sync::Arc<dyn kamu_resources::DeleteResourcesUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl $crate::ResourceCrudDispatcher for $dispatcher
        where
            $resource: kamu_resources::ReconcilableEventSourcedResource
                + kamu_resources::ResourceSchemaProvider
                + kamu_resources::ResourcePresentation,
            <$resource as kamu_resources::DeclarativeResource>::Spec: serde::Serialize,
            <$resource as kamu_resources::DeclarativeResource>::SpecInput:
                serde::de::DeserializeOwned,
            <$resource as kamu_resources::ReconcilableResource>::LifecycleError:
                std::error::Error + Send + Sync + 'static,
        {
            fn schema(&self) -> &'static kamu_resources::TypeUri {
                <$resource as kamu_resources::ResourceSchemaProvider>::schema()
            }

            async fn plan_apply(
                &self,
                request: $crate::ResourceCrudDispatcherApplyRequest,
            ) -> Result<
                kamu_resources::ApplyManifestPlanningDecision,
                $crate::ApplyResourceCrudDispatcherError,
            > {
                let schema = <$resource as kamu_resources::ResourceSchemaProvider>::schema();
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
                let schema = <$resource as kamu_resources::ResourceSchemaProvider>::schema();
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
            ) -> Result<kamu_resources::Resource, $crate::GetResourceCrudDispatcherError> {
                let state = self
                    .get_resource_by_id_use_case
                    .execute(request.account_id, &request.id)
                    .await
                    .map_err(kamu_resources::GetResourceCrudDispatcherError::from)?;

                $crate::typed_resource_state_to_resource::<$resource>(state).map_err(Into::into)
            }

            async fn list(
                &self,
                request: $crate::ResourceCrudDispatcherListRequest,
            ) -> Result<Vec<kamu_resources::ResourceSummaryView>, internal_error::InternalError> {
                let states = self
                    .list_resources_by_type_use_case
                    .execute(
                        request.account_id,
                        request.pagination,
                        request.label_filter,
                        request.query,
                    )
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
