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
            outbox: std::sync::Arc<dyn $crate::messaging_outbox::Outbox>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ApplyResourceUseCase<$resource> for $use_case {
            async fn plan(
                &self,
                params: kamu_resources::ApplyResourceParams<$resource>,
            ) -> Result<
                kamu_resources::ApplyResourcePlan<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
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
                kamu_resources::ApplyResourceResult<$resource>,
                kamu_resources::ApplyResourceUseCaseError<$resource>,
            > {
                let planner = $crate::ApplyResourcePlanner::<$resource>::new(
                    self.generic_resource_query_service.as_ref(),
                    self.typed_resource_query_service.as_ref(),
                    self.resource_aggregate_loader.as_ref(),
                    self.time_source.as_ref(),
                );
                let plan = planner.plan_internal(params).await?;

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
