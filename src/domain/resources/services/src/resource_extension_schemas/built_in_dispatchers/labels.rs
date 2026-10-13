// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_resources::{
    Environment,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_DOC,
    RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    RESOURCE_LABEL_ENVIRONMENT_SHORT_NAME,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ENVIRONMENT_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::AnyResource,
        preferred_name: Some(RESOURCE_LABEL_ENVIRONMENT_SHORT_NAME),
        aliases: &[],
    }];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

declare_resource_extension_schema_dispatcher!(
    dispatcher = LabelEnvironmentDispatcher,
    value = Environment,
    schema_id = RESOURCE_LABEL_ENVIRONMENT_SCHEMA_URI,
    kind = ResourceExtensionKind::Label,
    document = RESOURCE_LABEL_ENVIRONMENT_SCHEMA_DOC,
    applications = ENVIRONMENT_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_built_in_label_schema_dispatchers(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<LabelEnvironmentDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
