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
        ::paste::paste! {
            $crate::declare_resource_event_store_bridge!(
                bridge = [<$name EventStoreBridge>],
                store = $domain::[<$name EventStore>],
                resource = $resource,
                state = <$state_model as kamu_resources::ReconcilableStateModel>::State,
                event = <<$state_model as kamu_resources::ReconcilableStateModel>::State as event_sourcing::Projection>::Event
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

            $crate::declare_resource_query_service!(
                service = [<$name ResourceQueryServiceImpl>],
                resource = $resource
            );

            $crate::declare_apply_resource_use_case!(
                use_case = [<$name ApplyResourceUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_delete_resources_use_case!(
                use_case = [<$name DeleteResourcesUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_get_resource_by_uid_use_case!(
                use_case = [<$name GetResourceByUidUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_list_resources_by_kind_use_case!(
                use_case = [<$name ListResourcesByKindUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_reconcile_resource_use_case!(
                use_case = [<$name ReconcileResourceUseCaseImpl>],
                resource = $resource
            );

            $crate::declare_resource_lifecycle_reconcile_dispatcher!(
                dispatcher = [<$name ResourceLifecycleDispatcher>],
                resource = $resource
            );

            $crate::declare_resource_deletion_dispatcher!(
                dispatcher = [<$name ResourceDeletionDispatcher>],
                resource = $resource
            );

            pub fn [<register_ $name:snake _resource_service_layer>](
                catalog_builder: &mut dill::CatalogBuilder,
            ) {
                catalog_builder.add::<[<$name ApplyResourceUseCaseImpl>]>();
                catalog_builder.add::<[<$name DeleteResourcesUseCaseImpl>]>();
                catalog_builder.add::<[<$name EventStoreBridge>]>();
                catalog_builder.add::<[<$name ResourceAggregateLoaderImpl>]>();
                catalog_builder.add::<[<$name ResourceDeletionDispatcher>]>();
                catalog_builder.add::<[<$name ResourceLifecycleDispatcher>]>();
                catalog_builder.add::<[<$name ResourcePersistenceServiceImpl>]>();
                catalog_builder.add::<[<$name ResourceQueryServiceImpl>]>();
                catalog_builder.add::<[<$name GetResourceByUidUseCaseImpl>]>();
                catalog_builder.add::<[<$name ListResourcesByKindUseCaseImpl>]>();
                catalog_builder.add::<[<$name ReconcileResourceUseCaseImpl>]>();
            }
        }
    };

}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
