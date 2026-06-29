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
macro_rules! declare_resource_lifecycle_reconcile_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceLifecycleEventDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            descriptor: <$resource as kamu_resources::ResourceDescriptorProvider>::DESCRIPTOR,
        })]
        pub struct $dispatcher {
            reconcile_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ReconcileResourceUseCase<$resource>>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceLifecycleEventDispatcher for $dispatcher {
            async fn handle_applied(
                &self,
                resource: &kamu_resources::ResourceSnapshot,
            ) -> Result<(), internal_error::InternalError> {
                self.reconcile_resource_use_case
                    .execute(&resource.id)
                    .await
                    .map_err(internal_error::ErrorIntoInternal::int_err)
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

            async fn handle_deleted(&self, resource: &kamu_resources::ResourceSnapshot) -> Result<(), internal_error::InternalError> {
                Ok(())
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
