// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! declare_reconcile_resource_use_case {
    (
        use_case = $use_case:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ReconcileResourceUseCase<$resource>)]
        pub struct $use_case {
            event_store: std::sync::Arc<dyn $crate::domain::$store_trait>,
            reconciler: std::sync::Arc<dyn $crate::domain::Reconciler<$resource>>,
            time_source: std::sync::Arc<dyn time_source::SystemTimeSource>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ReconcileResourceUseCase<$resource> for $use_case {
            async fn execute(
                &self,
                id: &$crate::domain::ResourceID,
            ) -> Result<(), $crate::domain::ReconcileResourceUseCaseError<$resource>> {
                use $crate::domain::{DeclarativeResource, ReconcilableResource};

                let mut resource = <$resource>::load(id, self.event_store.as_ref())
                    .await
                    .map_err($crate::domain::ReconcileResourceUseCaseError::LoadFailed)?;
                if !resource.needs_reconciliation() {
                    return Ok(());
                }

                resource
                    .try_mark_reconciliation_started(self.time_source.now())
                    .map_err($crate::domain::ReconcileResourceUseCaseError::Lifecycle)?;

                resource
                    .save(self.event_store.as_ref())
                    .await
                    .map_err($crate::domain::ReconcileResourceUseCaseError::SaveFailed)?;

                match self.reconciler.reconcile(&resource).await {
                    Ok(success) => {
                        resource
                            .try_mark_reconciliation_succeeded(
                                self.time_source.now(),
                                resource.metadata().generation,
                                success,
                            )
                            .map_err($crate::domain::ReconcileResourceUseCaseError::Lifecycle)?;
                    }
                    Err(err) => {
                        resource
                            .try_mark_reconciliation_failed(
                                self.time_source.now(),
                                resource.metadata().generation,
                                &err,
                            )
                            .map_err($crate::domain::ReconcileResourceUseCaseError::Lifecycle)?;
                    }
                }

                resource
                    .save(self.event_store.as_ref())
                    .await
                    .map_err($crate::domain::ReconcileResourceUseCaseError::SaveFailed)?;

                Ok(())
            }
        }
    };
}

pub(crate) use declare_reconcile_resource_use_case;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
