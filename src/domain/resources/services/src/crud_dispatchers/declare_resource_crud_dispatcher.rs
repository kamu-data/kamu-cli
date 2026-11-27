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
macro_rules! declare_resource_crud_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::ResourceCrudDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource>::SCHEMA_STR,
            canonical_selector: <$resource>::CANONICAL_SELECTOR_NAME_STR,
            selector_aliases: <$resource>::SELECTOR_ALIAS_STRS,
        })]
        pub struct $dispatcher {
            apply_resource_use_case:
                std::sync::Arc<dyn kamu_resources::ApplyResourceUseCase<$resource>>,
            request_resources_deletion_use_case:
                std::sync::Arc<dyn kamu_resources::RequestResourcesDeletionUseCase<$resource>>,
            resource_aggregate_loader:
                std::sync::Arc<dyn kamu_resources::ResourceAggregateLoader<$resource>>,
            maybe_resource_operation_guard:
                Option<std::sync::Arc<dyn kamu_resources::ResourceOperationGuard<$resource>>>,
            default_resource_operation_guard: std::sync::Arc<$crate::DefaultResourceOperationGuard>,
        }

        impl $dispatcher {
            fn operation_guard(&self) -> &dyn kamu_resources::ResourceOperationGuard<$resource> {
                $crate::DefaultResourceOperationGuard::or_default(
                    self.maybe_resource_operation_guard.as_deref(),
                    self.default_resource_operation_guard.as_ref(),
                )
            }
        }

        #[async_trait::async_trait]
        impl $crate::ResourceCrudDispatcher for $dispatcher
        where
            $resource: kamu_resources::EventSourcedResource
                + kamu_resources::ResourceSchemaProvider
                + kamu_resources::ResourcePresentation,
            <$resource as kamu_resources::DeclarativeResource>::Spec: serde::Serialize,
            <$resource as kamu_resources::DeclarativeResource>::SpecInput:
                serde::de::DeserializeOwned,
            <$resource as kamu_resources::ManagedResource>::LifecycleError:
                std::error::Error + Send + Sync + 'static,
        {
            fn schema(&self) -> &'static kamu_resources::TypeUri {
                <$resource as kamu_resources::ResourceSchemaProvider>::type_uri()
            }

            async fn plan_apply(
                &self,
                request: $crate::ResourceCrudDispatcherApplyRequest,
            ) -> Result<
                kamu_resources::ApplyManifestPlanningDecision,
                $crate::ApplyResourceCrudDispatcherError,
            > {
                let params = $crate::decode_apply_resource_params::<$resource>(request)?;

                let plan = self
                    .apply_resource_use_case
                    .plan(params)
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_planning_decision::<$resource>(plan)
                    .map_err(Into::into)
            }

            async fn apply_with_precondition(
                &self,
                request: $crate::ResourceCrudDispatcherApplyRequest,
                precondition: kamu_resources::ApplyResourcePrecondition,
            ) -> Result<
                kamu_resources::ApplyManifestApplicationDecision,
                $crate::ApplyResourceCrudDispatcherError,
            > {
                let params = $crate::decode_apply_resource_params::<$resource>(request)?;

                let result = self
                    .apply_resource_use_case
                    .apply_with_precondition(params, precondition)
                    .await
                    .map_err(kamu_resources::ApplyResourceCrudDispatcherError::from)?;

                $crate::map_apply_resource_application_decision::<$resource>(result)
                    .map_err(Into::into)
            }

            async fn request_deletion(
                &self,
                request: $crate::ResourceCrudDispatcherDeletionRequest,
            ) -> Result<(), $crate::RequestResourcesDeletionCrudDispatcherError> {
                self.request_resources_deletion_use_case
                    .execute(request.account_id, request.ids)
                    .await
                    .map_err(kamu_resources::RequestResourcesDeletionCrudDispatcherError::from)
            }

            async fn check_account_access(
                &self,
                principal: &odf::metadata::auth::PrincipalHandle,
                account: &odf::ResourceOwnerHandle,
                operation: kamu_resources::ResourceAccountOperation,
            ) -> Result<kamu_resources::ResourceAccountAccess, kamu_resources::ResourceOperationError>
            {
                self.operation_guard()
                    .check_account(principal, account, operation)
                    .await
            }

            async fn check_resource_operations(
                &self,
                principal: &odf::metadata::auth::PrincipalHandle,
                account: &odf::ResourceOwnerHandle,
                operation: kamu_resources::ResourceAccountOperation,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<
                Vec<Result<(), kamu_resources::ResourceOperationError>>,
                internal_error::InternalError,
            > {
                $crate::check_resource_operations(
                    self.resource_aggregate_loader.as_ref(),
                    self.operation_guard(),
                    principal,
                    account,
                    operation,
                    ids,
                )
                .await
            }

            async fn check_list(
                &self,
                principal: &odf::metadata::auth::PrincipalHandle,
                account: &odf::ResourceOwnerHandle,
                scope: &kamu_resources::ResourceListScope,
            ) -> Result<(), kamu_resources::ResourceOperationError> {
                self.operation_guard()
                    .check_operation(
                        principal,
                        &kamu_resources::ResourceOperationRequest {
                            account,
                            operation: kamu_resources::ResourceOperation::List { scope },
                        },
                    )
                    .await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
