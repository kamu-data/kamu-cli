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
macro_rules! declare_resource_lifecycle_reconcile_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceLifecycleEventDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource>::SCHEMA_STR,
            canonical_selector: <$resource>::CANONICAL_SELECTOR_NAME_STR,
            selector_aliases: <$resource>::SELECTOR_ALIAS_STRS,
        })]
        pub struct $dispatcher {
            reconcile_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ReconcileResourceUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceLifecycleEventDispatcher for $dispatcher
        where
            $resource: kamu_resources::ResourcePresentation,
        {
            async fn handle_applied(
                &self,
                resource: &kamu_resources::ResourceSnapshot,
            ) -> Result<(), internal_error::InternalError> {
                use kamu_resources::ReconcileResourceUseCaseError as E;

                match self.reconcile_resource_use_case.execute(&resource.id).await {
                    Ok(()) => Ok(()),
                    // Another reconciliation of the same resource records its
                    // own outcome
                    Err(E::StatusConflict(err)) => {
                        tracing::info!(
                            resource_id = %resource.id,
                            error = %err,
                            "Skipping reconciliation outcome: status was updated concurrently"
                        );
                        Ok(())
                    }
                    // The other writer is an apply, which posts its own `Applied`
                    // message, a delete, or another reconciliation
                    Err(E::ConcurrentModification(err)) => {
                        tracing::info!(
                            resource_id = %resource.id,
                            error = %err,
                            "Skipping reconciliation: resource was modified concurrently"
                        );
                        Ok(())
                    }
                    Err(err @ (E::LoadFailed(_) | E::Internal(_) | E::Lifecycle(_))) => {
                        Err(internal_error::ErrorIntoInternal::int_err(err))
                    }
                }
            }

            async fn handle_reconciliation_succeeded(
                &self,
                _resource: &kamu_resources::ResourceSnapshot,
            ) -> Result<(), internal_error::InternalError> {
                Ok(())
            }

            async fn handle_reconciliation_failed(
                &self,
                _resource: &kamu_resources::ResourceSnapshot,
            ) -> Result<(), internal_error::InternalError> {
                Ok(())
            }

            async fn handle_deleted(
                &self,
                _resource: &kamu_resources::ResourceSnapshot,
            ) -> Result<(), internal_error::InternalError> {
                Ok(())
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
