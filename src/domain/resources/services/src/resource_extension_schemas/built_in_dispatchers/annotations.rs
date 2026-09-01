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
    Description,
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_DOC,
    RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    RESOURCE_ANNOTATION_DESCRIPTION_SHORT_NAME,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DESCRIPTION_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::AnyResource,
        preferred_name: Some(RESOURCE_ANNOTATION_DESCRIPTION_SHORT_NAME),
        aliases: &[],
    }];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

declare_resource_extension_schema_dispatcher!(
    dispatcher = DescriptionAnnotationDispatcher,
    value = Description,
    schema_id = RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_URI,
    kind = ResourceExtensionKind::Annotation,
    document = RESOURCE_ANNOTATION_DESCRIPTION_SCHEMA_DOC,
    applications = DESCRIPTION_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_built_in_annotation_schema_dispatchers(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<DescriptionAnnotationDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
