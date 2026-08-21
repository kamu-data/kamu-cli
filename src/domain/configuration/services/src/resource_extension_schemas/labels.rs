// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_configuration::{
    LegacyConfigTargetDataset,
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_DOC,
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SHORT_NAME,
};
use kamu_resources::{
    ResourceExtensionApplicationMeta,
    ResourceExtensionKind,
    ResourceExtensionScopeMeta,
};
use kamu_resources_services::declare_resource_extension_schema_dispatcher;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Scoped to the ODF config context, so the label resolves on `VariableSet`
/// and `SecretSet` but is rejected as inapplicable anywhere else. `version` is
/// left open so the label survives a config-schema version bump.
const LEGACY_CONFIG_TARGET_DATASET_APPLICATIONS: &[ResourceExtensionApplicationMeta] =
    &[ResourceExtensionApplicationMeta {
        scope: ResourceExtensionScopeMeta::ResourceContext {
            authority: "https://opendatafabric.org/schemas",
            context: "config",
            version: None,
        },
        preferred_name: Some(RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SHORT_NAME),
        aliases: &[],
    }];

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

declare_resource_extension_schema_dispatcher!(
    dispatcher = LabelLegacyConfigTargetDatasetDispatcher,
    value = LegacyConfigTargetDataset,
    schema_id = RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_URI,
    kind = ResourceExtensionKind::Label,
    document = RESOURCE_LABEL_LEGACY_CONFIG_TARGET_DATASET_SCHEMA_DOC,
    applications = LEGACY_CONFIG_TARGET_DATASET_APPLICATIONS
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_configuration_label_schema_dispatchers(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<LabelLegacyConfigTargetDatasetDispatcher>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
