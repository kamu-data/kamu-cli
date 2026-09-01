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
        store = $store:path
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceAggregateLoader<$resource>)]
        pub struct $loader {
            event_store: std::sync::Arc<dyn $store>,
        }

        #[async_trait::async_trait]
        impl kamu_resources::ResourceAggregateLoader<$resource> for $loader {
            async fn load(
                &self,
                id: &kamu_resources::ResourceID,
            ) -> Result<
                $resource,
                event_sourcing::LoadError<
                    <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                >,
            > {
                <$resource>::load(id, self.event_store.as_ref()).await
            }

            async fn load_many(
                &self,
                ids: &[kamu_resources::ResourceID],
            ) -> Result<
                Vec<
                    Result<
                        $resource,
                        event_sourcing::LoadError<
                            <$resource as kamu_resources::DeclarativeResource>::ResourceState,
                        >,
                    >,
                >,
                event_sourcing::GetEventsError,
            > {
                <$resource>::load_multi(ids, self.event_store.as_ref()).await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
