// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// Generates the main lifecycle controller of a resource type, a component
/// that runs `SimpleResourceController` for it
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
                $crate::SimpleResourceController::<$resource>::new(
                    &self.catalog,
                    self.time_source.as_ref(),
                )
                .process(*id)
                .await
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
