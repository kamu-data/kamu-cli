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
macro_rules! declare_resource_presentation_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourcePresentationDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource>::SCHEMA_STR,
            name: <$resource as kamu_resources::ResourcePresentation>::PRESENTATION.resource_name,
            short_names:
                <$resource as kamu_resources::ResourcePresentation>::PRESENTATION.resource_short_names,
        })]
        pub struct $dispatcher;

        impl kamu_resources::ResourcePresentationDispatcher for $dispatcher
        where
            $resource: kamu_resources::ResourceSchemaProvider + kamu_resources::ResourcePresentation,
        {
            fn schema(&self) -> &'static kamu_resources::TypeUri {
                <$resource as kamu_resources::ResourceSchemaProvider>::schema()
            }

            fn presentation(&self) -> kamu_resources::ResourcePresentationDefinition {
                <$resource as kamu_resources::ResourcePresentation>::PRESENTATION
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
