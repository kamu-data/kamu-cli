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
    RESOURCE_CONDITION_READY_SCHEMA_DOC,
    RESOURCE_CONDITION_READY_SCHEMA_URI,
    RESOURCE_CONDITION_RECONCILING_SCHEMA_DOC,
    RESOURCE_CONDITION_RECONCILING_SCHEMA_URI,
    ReadyCondition,
    ReconcilingCondition,
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CONDITION_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::AnyResource,
        preferred_name: None,
        aliases: &[],
    }];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

declare_resource_extension_schema_dispatcher!(
    dispatcher = ReadyConditionDispatcher,
    value = ReadyCondition,
    schema_id = RESOURCE_CONDITION_READY_SCHEMA_URI,
    kind = ResourceExtensionKind::Condition,
    document = RESOURCE_CONDITION_READY_SCHEMA_DOC,
    applications = CONDITION_APPLICATIONS
);

declare_resource_extension_schema_dispatcher!(
    dispatcher = ReconcilingConditionDispatcher,
    value = ReconcilingCondition,
    schema_id = RESOURCE_CONDITION_RECONCILING_SCHEMA_URI,
    kind = ResourceExtensionKind::Condition,
    document = RESOURCE_CONDITION_RECONCILING_SCHEMA_DOC,
    applications = CONDITION_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_built_in_condition_schema_dispatchers(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<ReadyConditionDispatcher>();
    catalog_builder.add::<ReconcilingConditionDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
