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
macro_rules! declare_apply_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ApplyResourceUseCase<$resource>)]
        pub struct $use_case {
            generic_resource_query_service:
                std::sync::Arc<dyn kamu_resources::GenericResourceQueryService>,
            typed_resource_query_service:
                std::sync::Arc<dyn kamu_resources::TypedResourceQueryService<$resource>>,
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            resource_persistence_service:
                std::sync::Arc<dyn kamu_resources::ResourcePersistenceService<$resource>>,
            resource_spec_sanitizer:
                Option<std::sync::Arc<dyn kamu_resources::ResourceSpecSanitizer<$resource>>>,
            outbox: std::sync::Arc<dyn $crate::messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            async fn sanitize_spec(
                &self,
                spec: <$resource as kamu_resources::DeclarativeResource>::Spec,
            ) -> Result<
                <$resource as kamu_resources::DeclarativeResource>::Spec,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                if let Some(sanitizer) = &self.resource_spec_sanitizer {
                    sanitizer
                        .sanitize(spec)
                        .await
                        .map_err(kamu_resources::ApplyResourceUseCaseError::Internal)
                } else {
                    Ok(spec)
                }
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::ApplyResourceUseCase<$resource> for $use_case
        where
            $resource: kamu_resources::ReconcilableEventSourcedResource
                + kamu_resources::ResourceDescriptorProvider,
            <$resource as kamu_resources::DeclarativeResource>::Spec:
                serde::Serialize + PartialEq + Clone + kamu_resources::ResourceValidateSpec,
            <$resource as kamu_resources::DeclarativeResource>::Status:
                serde::Serialize + kamu_resources::ResourceStatusLike,
            <$resource as kamu_resources::ReconcilableResource>::LifecycleError:
                kamu_resources::IntoApplyResourceRejection
                    + kamu_resources::InvariantViolationOf<
                        <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                    >
                    + From<kamu_resources::ResourceMetadataValidationError>
                    + From<
                        <<$resource as kamu_resources::DeclarativeResource>::Spec as kamu_resources::ResourceValidateSpec>::ValidationError,
                    >,
        {
            async fn plan(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourcePlanningDecision<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                let sanitized_spec = self.sanitize_spec(params.spec).await?;
                let params = kamu_resources::ApplyResourceParams {
                    spec: sanitized_spec,
                    ..params
                };

                let planner = $crate::ApplyResourcePlanner::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.typed_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.time_source.as_ref(),
                );

                planner.plan(params).await
            }

            async fn apply(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourceApplicationDecision<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                let sanitized_spec = self.sanitize_spec(params.spec).await?;
                let params = kamu_resources::ApplyResourceParams {
                    spec: sanitized_spec,
                    ..params
                };

                let planner = $crate::ApplyResourcePlanner::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.typed_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.time_source.as_ref(),
                );
                let plan = planner.plan_internal(params).await?;

                let plan = match plan {
                    $crate::PlannedApplyResourceDecision::Planned(plan) => plan,
                    $crate::PlannedApplyResourceDecision::Rejected(rejection) => {
                        return Ok(kamu_resources::ApplyResourceApplicationDecision::Rejected(
                            rejection,
                        ));
                    }
                };

                let executor = $crate::ApplyResourcePlanExecutor::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.typed_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.resource_persistence_service.as_ref(),
                    self.outbox.as_ref(),
                    self.time_source.as_ref(),
                );

                executor.execute(plan).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
