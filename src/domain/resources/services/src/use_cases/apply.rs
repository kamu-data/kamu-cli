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
            maybe_resource_spec_pre_apply:
                Option<std::sync::Arc<dyn kamu_resources::ResourceSpecPreApply<$resource>>>,
            maybe_resource_placement:
                Option<std::sync::Arc<dyn kamu_resources::ResourcePlacement<$resource>>>,
            maybe_resource_operation_guard:
                Option<std::sync::Arc<dyn kamu_resources::ResourceOperationGuard<$resource>>>,
            default_resource_operation_guard: std::sync::Arc<$crate::DefaultResourceOperationGuard>,
            name_suffix_generator:
                std::sync::Arc<dyn kamu_resources::ResourceNameSuffixGenerator>,
            outbox: std::sync::Arc<dyn $crate::messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        impl $use_case {
            fn runner(&self) -> $crate::ApplyResourceRunner<'_, $resource> {
                $crate::ApplyResourceRunner {
                    generic_resource_query_service: self.generic_resource_query_service.as_ref(),
                    typed_resource_query_service: self.typed_resource_query_service.as_ref(),
                    resource_aggregate_loader: self.resource_aggregate_loader.as_ref(),
                    resource_persistence_service: self.resource_persistence_service.as_ref(),
                    maybe_resource_spec_pre_apply: self.maybe_resource_spec_pre_apply.as_deref(),
                    maybe_resource_placement: self.maybe_resource_placement.as_deref(),
                    maybe_resource_operation_guard: self.maybe_resource_operation_guard.as_deref(),
                    default_resource_operation_guard: self.default_resource_operation_guard.as_ref(),
                    name_suffix_generator: self.name_suffix_generator.as_ref(),
                    outbox: self.outbox.as_ref(),
                    time_source: self.time_source.as_ref(),
                }
            }
        }

        #[async_trait::async_trait]
        impl kamu_resources::ApplyResourceUseCase<$resource> for $use_case
        where
            $resource: kamu_resources::EventSourcedResource
                + kamu_resources::ResourceSchemaProvider,
            <$resource as kamu_resources::DeclarativeResource>::Spec:
                serde::Serialize + PartialEq + Clone + kamu_resources::ResourceSpecFromInput<
                    <$resource as kamu_resources::DeclarativeResource>::SpecInput,
                >,
            <$resource as kamu_resources::DeclarativeResource>::SpecInput:
                serde::Serialize + Clone + kamu_resources::ResourceValidateSpec + kamu_resources::ResourceLinterSpec,
            <$resource as kamu_resources::ManagedResource>::LifecycleError:
                kamu_resources::IntoApplyResourceRejection
                    + kamu_resources::InvariantViolationOf<
                        <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                    >
                    + From<kamu_resources::ResourceHeadersValidationError>
                    + From<
                        <<$resource as kamu_resources::DeclarativeResource>::SpecInput as kamu_resources::ResourceValidateSpec>::ValidationError,
                    >,
        {
            async fn plan(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourcePlanningDecision<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                self.runner().plan(params).await
            }

            async fn apply(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourceApplicationDecision<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                self.apply_with_precondition(
                    params,
                    kamu_resources::ApplyResourcePrecondition::None,
                )
                .await
            }

            async fn apply_with_precondition(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
                precondition: kamu_resources::ApplyResourcePrecondition,
            ) -> Result<
                kamu_resources::ApplyResourceApplicationDecision<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                self.runner().apply(params, precondition).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
