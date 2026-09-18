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
macro_rules! declare_resource_extension_schema_dispatcher {
    (
        dispatcher = $dispatcher:ident,
        value = $value:ty,
        schema_id = $schema_id:expr,
        kind = $kind:expr,
        document = $document:expr,
        applications = $applications:expr
    ) => {
        #[dill::component]
        #[dill::interface(dyn kamu_resources::ResourceExtensionSchemaDispatcher)]
        #[dill::meta(kamu_resources::ResourceExtensionSchemaMeta {
            schema_id: $schema_id,
            kind: $kind,
            document: $document,
            applications: $applications,
        })]
        pub struct $dispatcher;

        impl kamu_resources::ResourceExtensionSchemaDispatcher for $dispatcher {
            fn validate_value(
                &self,
                value: &serde_json::Value,
            ) -> Result<(), kamu_resources::ResourceExtensionValueError> {
                <$value as kamu_resources::ResourceValidateSchemaValue>::validate(value)
                    .map_err(kamu_resources::ResourceExtensionValueError::from_error)
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
