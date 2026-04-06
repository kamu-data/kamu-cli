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
            descriptor: <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR,
        })]
        pub struct $dispatcher {
            apply_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ApplyResourceUseCase<$resource>>,
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            get_resource_by_uid_use_case:
                std::sync::Arc<dyn kamu_resources::GetResourceByUidUseCase<$resource>>,
            list_resources_by_kind_use_case:
                std::sync::Arc<dyn kamu_resources::ListResourcesByKindUseCase<$resource>>,
            delete_resources_use_case:
                std::sync::Arc<dyn kamu_resources::DeleteResourcesUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl $crate::ResourceCrudDispatcher for $dispatcher
        where
            $resource: kamu_resources::ReconcilableEventSourcedResource
                + kamu_resources::ResourceDescriptorProvider,
            <$resource as kamu_resources::DeclarativeResource>::Spec:
                serde::de::DeserializeOwned
                    + serde::Serialize
                    + kamu_resources::ResourceValidateSpec
                    + kamu_resources::ResourceLinterSpec,
            <<$resource as kamu_resources::DeclarativeResource>::Spec as kamu_resources::ResourceValidateSpec>::ValidationError:
                std::fmt::Display,
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
                let kind =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.resource_type;
                let api_version =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.api_version;
                let decoded_spec =
                    $crate::decode_resource_spec::<$resource>(kind, api_version, request.spec)?;

                let plan = self
                    .apply_resource_use_case
                    .plan(kamu_resources::ApplyResourceParams {
                        uid: request.uid,
                        metadata: request.metadata,
                        spec: decoded_spec.spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_planning_decision::<$resource>(
                    plan,
                    self.generic_resource_query_service.as_ref(),
                    decoded_spec.warnings,
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
                let kind =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.resource_type;
                let api_version =
                    <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR.api_version;
                let decoded_spec =
                    $crate::decode_resource_spec::<$resource>(kind, api_version, request.spec)?;

                let result = self
                    .apply_resource_use_case
                    .apply(kamu_resources::ApplyResourceParams {
                        uid: request.uid,
                        metadata: request.metadata,
                        spec: decoded_spec.spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_application_decision::<$resource>(
                    result,
                    decoded_spec.warnings,
                )
                    .map_err(Into::into)
            }

            async fn get(
                &self,
                request: $crate::ResourceCrudDispatcherGetRequest,
            ) -> Result<kamu_resources::ResourceView, $crate::GetResourceCrudDispatcherError> {
                let state = self
                    .get_resource_by_uid_use_case
                    .execute(request.account_id, &request.uid)
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
                    .execute(request.account_id, request.uids)
                    .await
                    .map_err(kamu_resources::DeleteResourcesCrudDispatcherError::from)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
