// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::message_handlers::{
    ConfigurationDatasetLifecycleMessageConsumer,
    ConfigurationResourceLifecycleMessageConsumer,
};
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<ConfigurationDatasetLifecycleMessageConsumer>();
    catalog_builder.add::<ConfigurationResourceLifecycleMessageConsumer>();

    register_variable_set_resource_service_layer(catalog_builder);
    register_variable_set_resource_crud_dispatcher(catalog_builder);
    catalog_builder.add::<VariableSetReconcilerImpl>();

    register_secret_set_resource_service_layer(catalog_builder);
    register_secret_set_resource_crud_dispatcher(catalog_builder);
    catalog_builder.add::<SecretSetReconcilerImpl>();
    catalog_builder.add::<SecretSetSpecSanitizer>();
    catalog_builder.add::<SecretSetSpecViewDispatcher>();

    catalog_builder.add::<DatasetEnvVarResolverImpl>();
    catalog_builder.add::<DatasetEnvVarMutationAdapterImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
