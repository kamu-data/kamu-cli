// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Generates the main lifecycle controller of a resource type, which runs its
/// `SimpleResourceReconciler` in transactions of its own
#[macro_export]
macro_rules! declare_simple_resource_controller_adapter {
    (
        adapter = $adapter:ident,
        resource = $resource:ty,
        controller_name = $controller_name:expr,
        controller_revision = $controller_revision:expr
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceController)]
        #[dill::meta(kamu_resources::ResourceControllerMeta {
            schema: <$resource>::SCHEMA_STR,
            controller_name: kamu_resources::ResourceControllerName::new($controller_name),
            controller_revision: $controller_revision,
            reacts_to_status: false,
        })]
        pub struct $adapter {
            catalog: dill::Catalog,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceController for $adapter {
            async fn process(
                &self,
                id: &kamu_resources::ResourceID,
            ) -> Result<kamu_resources::ResourceProcessOutcome, internal_error::InternalError> {
                $crate::SimpleResourceControllerSteps::<$resource>::process_resource(self, *id).await
            }
        }

        #[async_trait::async_trait]
        impl $crate::SimpleResourceControllerSteps<$resource> for $adapter {
            fn now(&self) -> chrono::DateTime<chrono::Utc> {
                self.time_source.now()
            }

            async fn load(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<$resource, internal_error::InternalError> {
                self.load_in_transaction(id).await
            }

            async fn start_reconciliation(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::ResourceLifecycleTransitionError<$resource>>
            {
                self.start_reconciliation_phase(id).await
            }

            async fn finish_reconciliation(
                &self,
                started: $resource,
            ) -> Result<
                $crate::SimpleResourceReconciliationOutcome<$resource>,
                kamu_resources::SimpleResourceReconciliationError<$resource>,
            > {
                self.finish_reconciliation_phase(started).await
            }

            async fn run_cleanup(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<
                $crate::SimpleResourceCleanupOutcome,
                $crate::ResourceCleanupTransactionError<$resource>,
            > {
                self.cleanup_in_transaction(id).await
            }

            async fn complete_deletion(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::SimpleResourceDeletionError<$resource>>
            {
                self.complete_deletion_in_transaction(id).await
            }

            async fn record_cleanup_failure(
                &self,
                failure: kamu_resources::ResourceCleanupFailure,
            ) -> Result<(), kamu_resources::SimpleResourceDeletionError<$resource>> {
                self.record_cleanup_failure_in_transaction(failure).await
            }
        }

        impl $adapter {
            #[::database_common_macros::transactional_method1(
                resource_aggregate_loader:
                    std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>
            )]
            async fn load_in_transaction(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<$resource, internal_error::InternalError> {
                internal_error::ResultIntoInternal::int_err(resource_aggregate_loader.load(&id).await)
            }

            /// Commits the started reconciliation before the reconciler runs,
            /// a stable hand-off point for the second phase
            #[::database_common_macros::transactional_method1(
                lifecycle_primitives:
                    std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>
            )]
            async fn start_reconciliation_phase(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::ResourceLifecycleTransitionError<$resource>>
            {
                lifecycle_primitives.begin_reconciliation(&id, None).await
            }

            #[::database_common_macros::transactional_method2(
                lifecycle_primitives:
                    std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>,
                reconciler:
                    std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
            )]
            async fn finish_reconciliation_phase(
                &self,
                resource: $resource,
            ) -> Result<
                $crate::SimpleResourceReconciliationOutcome<$resource>,
                kamu_resources::SimpleResourceReconciliationError<$resource>,
            > {
                $crate::SimpleResourceReconciliationHelper::<$resource>::new(
                    lifecycle_primitives.as_ref(),
                    reconciler.as_ref(),
                )
                .finish_reconciliation_phase(resource)
                .await
            }

            #[::database_common_macros::transactional_method2(
                resource_aggregate_loader:
                    std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
                reconciler:
                    std::sync::Arc<dyn kamu_resources::SimpleResourceReconciler<$resource>>
            )]
            async fn cleanup_in_transaction(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<
                $crate::SimpleResourceCleanupOutcome,
                $crate::ResourceCleanupTransactionError<$resource>,
            > {
                $crate::SimpleResourceCleanupHelper::<$resource>::new(
                    resource_aggregate_loader.as_ref(),
                    reconciler.as_ref(),
                )
                .cleanup(id)
                .await
            }

            #[::database_common_macros::transactional_method1(
                lifecycle_primitives:
                    std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>
            )]
            async fn complete_deletion_in_transaction(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::SimpleResourceDeletionError<$resource>>
            {
                $crate::SimpleResourceDeletionRecorder::<$resource>::new(lifecycle_primitives.as_ref())
                    .complete_deletion(id)
                    .await
            }

            #[::database_common_macros::transactional_method1(
                lifecycle_primitives:
                    std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>
            )]
            async fn record_cleanup_failure_in_transaction(
                &self,
                failure: kamu_resources::ResourceCleanupFailure,
            ) -> Result<(), kamu_resources::SimpleResourceDeletionError<$resource>> {
                $crate::SimpleResourceDeletionRecorder::<$resource>::new(lifecycle_primitives.as_ref())
                    .record_cleanup_failure(failure)
                    .await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
