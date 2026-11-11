// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Generates the `SimpleResourceController` of a resource type, which runs its
/// `SimpleResourceReconciler` in transactions of its own
#[macro_export]
macro_rules! declare_simple_resource_controller_adapter {
    (
        adapter = $adapter:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::SimpleResourceController<$resource>)]
        pub struct $adapter {
            catalog: dill::Catalog,
        }

        #[async_trait::async_trait]
        impl kamu_resources::SimpleResourceController<$resource> for $adapter {
            async fn reconcile(
                &self,
                id: &kamu_resources::ResourceID,
            ) -> Result<(), kamu_resources::SimpleResourceReconciliationError<$resource>> {
                let Some(resource) = self.start_reconciliation_phase(*id).await? else {
                    return Ok(());
                };

                self.finish_reconciliation_phase(resource).await
            }

            async fn complete_deletions(
                &self,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<(), kamu_resources::SimpleResourceDeletionError<$resource>> {
                $crate::SimpleResourceDeletionPhases::run_deletion_phases(self, ids).await
            }
        }

        impl $adapter {
            /// Commits the started reconciliation before the reconciler runs,
            /// a stable hand-off point for the second phase
            #[::database_common_macros::transactional_method1(
                lifecycle_primitives:
                    std::sync::Arc<dyn kamu_resources::ResourceLifecyclePrimitives<$resource>>
            )]
            async fn start_reconciliation_phase(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<Option<$resource>, kamu_resources::SimpleResourceReconciliationError<$resource>>
            {
                Ok(lifecycle_primitives.begin_reconciliation(&id, None).await?)
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
            ) -> Result<(), kamu_resources::SimpleResourceReconciliationError<$resource>> {
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
                Option<$crate::ResourceCleanupAttempt>,
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
            async fn record_in_transaction(
                &self,
                attempts: Vec<$crate::ResourceCleanupAttempt>,
            ) -> Result<(), kamu_resources::SimpleResourceDeletionError<$resource>> {
                $crate::SimpleResourceDeletionRecorder::<$resource>::new(lifecycle_primitives.as_ref())
                    .record_outcomes(&attempts)
                    .await
            }
        }
        #[async_trait::async_trait]
        impl $crate::SimpleResourceDeletionPhases<$resource> for $adapter {
            async fn run_cleanup(
                &self,
                id: kamu_resources::ResourceID,
            ) -> Result<
                Option<$crate::ResourceCleanupAttempt>,
                $crate::ResourceCleanupTransactionError<$resource>,
            > {
                self.cleanup_in_transaction(id).await
            }

            async fn record_outcomes(
                &self,
                attempts: Vec<$crate::ResourceCleanupAttempt>,
            ) -> Result<(), kamu_resources::SimpleResourceDeletionError<$resource>> {
                self.record_in_transaction(attempts).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
