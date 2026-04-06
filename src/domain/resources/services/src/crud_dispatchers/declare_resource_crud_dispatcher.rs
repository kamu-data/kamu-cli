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
                    + kamu_resources::ResourceValidateSpec,
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
                let spec = $crate::decode_resource_spec::<$resource>(kind, api_version, request.spec)?;

                let plan = self
                    .apply_resource_use_case
                    .plan(kamu_resources::ApplyResourceParams {
                        uid: request.uid,
                        metadata: request.metadata,
                        spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                Ok(match plan {
                    kamu_resources::ApplyResourcePlanningDecision::Planned(plan) => {
                        let resource = $crate::typed_resource_state_to_view::<$resource>(&plan.state)?;

                        kamu_resources::ApplyManifestPlanningDecision::Planned(
                            kamu_resources::ApplyManifestPlan {
                                resource,
                                outcome: plan.action.into(),
                                reconciliation_required: plan.reconciliation_required,
                                executable: plan.executable,
                            },
                        )
                    }
                    kamu_resources::ApplyResourcePlanningDecision::Rejected(rejection) => {
                        kamu_resources::ApplyManifestPlanningDecision::Rejected(rejection.into())
                    }
                })
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
                let spec = $crate::decode_resource_spec::<$resource>(kind, api_version, request.spec)?;

                let result = self
                    .apply_resource_use_case
                    .apply(kamu_resources::ApplyResourceParams {
                        uid: request.uid,
                        metadata: request.metadata,
                        spec,
                    })
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                Ok(match result {
                    kamu_resources::ApplyResourceApplicationDecision::Applied(result) => {
                        let resource =
                            $crate::typed_resource_state_to_view::<$resource>(&result.state)?;

                        kamu_resources::ApplyManifestApplicationDecision::Applied(
                            kamu_resources::ApplyManifestResult {
                                resource,
                                outcome: result.outcome,
                            },
                        )
                    }
                    kamu_resources::ApplyResourceApplicationDecision::Rejected(rejection) => {
                        kamu_resources::ApplyManifestApplicationDecision::Rejected(rejection.into())
                    }
                })
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

                $crate::typed_resource_state_to_view::<$resource>(&state).map_err(Into::into)
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
