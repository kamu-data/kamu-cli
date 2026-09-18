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
macro_rules! declare_resource_presentation_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        resource = $resource:ty
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourcePresentationDispatcher)]
        #[dill::meta(kamu_resources::ResourceDispatcherMeta {
            schema: <$resource>::SCHEMA_STR,
            canonical_selector: <$resource>::CANONICAL_SELECTOR_NAME_STR,
            selector_aliases: <$resource>::SELECTOR_ALIAS_STRS,
        })]
        pub struct $dispatcher;

        impl kamu_resources::ResourcePresentationDispatcher for $dispatcher
        where
            $resource:
                kamu_resources::ResourceSchemaProvider + kamu_resources::ResourcePresentation,
        {
            fn schema(&self) -> &'static kamu_resources::TypeUri {
                <$resource as kamu_resources::ResourceSchemaProvider>::schema()
            }

            fn presentation(&self) -> kamu_resources::ResourcePresentationDefinition {
                <$resource as kamu_resources::ResourcePresentation>::PRESENTATION.clone()
            }

            fn list_column_values_for_snapshot(
                &self,
                snapshot: &kamu_resources::ResourceSnapshot,
            ) -> Result<
                Vec<kamu_resources::ResourceListColumnValueView>,
                internal_error::InternalError,
            > {
                use internal_error::ErrorIntoInternal;

                // Guards against a caller routing a snapshot to the wrong
                // dispatcher, which would otherwise surface as a confusing
                // deserialization failure — or, for two types with
                // structurally compatible specs, as silently wrong columns.
                let expected =
                    <$resource as kamu_resources::ResourceSchemaProvider>::schema();
                if snapshot.schema != *expected {
                    return Err(format!(
                        "Presentation dispatcher for '{expected}' received a snapshot of type \
                         '{}'",
                        snapshot.schema
                    )
                    .int_err());
                }

                // `ResourceState: TryFrom<ResourceSnapshot>` is the same
                // conversion the typed read path uses, so columns computed
                // here agree with the ones the dispatcher produces.
                let state = <$resource as kamu_resources::DeclarativeResource>::ResourceState
                    ::try_from(snapshot.clone())?;

                Ok(<$resource as kamu_resources::ResourcePresentation>::list_column_values(
                    &state,
                ))
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
