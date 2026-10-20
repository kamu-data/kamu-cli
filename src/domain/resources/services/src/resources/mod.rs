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
macro_rules! declare_resource_service_layer {
    (
        domain = $domain:path,
        name = $name:ident,
        resource = $resource:ty,
        state_model = $state_model:ty
    ) => {
        $crate::paste::paste! {
            $crate::declare_resource_event_store_bridge!(
                bridge = [<$name EventStoreBridge>],
                store = $domain::[<$name EventStore>],
                resource = $resource,
                state = <$state_model as kamu_resources::ResourceStateModel>::State,
                event = <<$state_model as kamu_resources::ResourceStateModel>::State as event_sourcing::Projection>::Event
            );

            $crate::declare_resource_aggregate_loader!(
                loader = [<$name ResourceAggregateLoaderImpl>],
                resource = $resource,
                store = $domain::[<$name EventStore>]
            );

            $crate::declare_resource_persistence_service!(
                service = [<$name ResourcePersistenceServiceImpl>],
                resource = $resource,
                store = $domain::[<$name EventStore>]
            );

            $crate::declare_typed_resource_query_service!(
                service = [<$name TypedResourceQueryServiceImpl>],
                resource = $resource
            );

            $crate::declare_apply_resource_use_case!(
                use_case = [<$name ApplyResourceUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_request_resources_deletion_use_case!(
                use_case = [<$name RequestResourcesDeletionUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_complete_resource_deletion_use_case!(
                use_case = [<$name CompleteResourceDeletionUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_reconcile_resource_use_case!(
                use_case = [<$name ReconcileResourceUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_resource_lifecycle_controller_dispatcher!(
                dispatcher = [<$name ResourceLifecycleDispatcher>],
                resource = $resource
            );

            pub fn [<register_ $name:snake _resource_service_layer>](
                catalog_builder: &mut dill::CatalogBuilder,
            ) {
                catalog_builder.add::<[<$name ApplyResourceUseCaseImpl>]>();
                catalog_builder.add::<[<$name CompleteResourceDeletionUseCaseImpl>]>();
                catalog_builder.add::<[<$name RequestResourcesDeletionUseCaseImpl>]>();
                catalog_builder.add::<[<$name EventStoreBridge>]>();
                catalog_builder.add::<[<$name ResourceAggregateLoaderImpl>]>();
                catalog_builder.add::<[<$name ResourceLifecycleDispatcher>]>();
                catalog_builder.add::<[<$name ResourcePersistenceServiceImpl>]>();
                catalog_builder.add::<[<$name TypedResourceQueryServiceImpl>]>();
                catalog_builder.add::<[<$name ReconcileResourceUseCaseImpl>]>();
            }
        }
    };

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
