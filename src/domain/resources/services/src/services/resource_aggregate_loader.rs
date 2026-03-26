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
macro_rules! declare_resource_aggregate_loader {
    (
        loader = $loader:ident,
        resource = $resource:ty,
        store = $store_trait:ident
    ) => {
        #[dill::component]
        #[dill::interface(dyn $crate::domain::ResourceAggregateLoader<$resource>)]
        pub struct $loader {
            event_store: std::sync::Arc<dyn $crate::domain::$store_trait>,
        }

        #[async_trait::async_trait]
        impl $crate::domain::ResourceAggregateLoader<$resource> for $loader {
            async fn load(
                &self,
                resource_id: &$crate::domain::ResourceID,
            ) -> Result<
                $resource,
                event_sourcing::LoadError<
                    <$resource as $crate::domain::DeclarativeResource>::ResourceState,
                >,
            > {
                <$resource>::load(resource_id, self.event_store.as_ref()).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
