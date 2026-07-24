// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<GenericResourceQueryServiceImpl>();
    catalog_builder.add::<DeleteAccountResourcesUsecaseImpl>();
    catalog_builder.add::<ListAllResourcesUseCaseImpl>();
    catalog_builder.add::<crate::message_handlers::AccountLifecycleMessageConsumer>();
    catalog_builder.add::<crate::message_handlers::ResourceLifecycleMessageConsumer>();

    register_built_in_extension_schema_dispatchers(catalog_builder);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn build_catalog_with_resource_extension_schema_registry(
    mut catalog_builder: CatalogBuilder,
) -> Result<Catalog, InternalError> {
    let mut preview_builder = catalog_builder.clone();
    let preview_catalog = preview_builder.build();
    let registry = ResourceExtensionSchemaRegistry::try_new(&preview_catalog)?;
    catalog_builder.add_value(registry);
    Ok(catalog_builder.build())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
