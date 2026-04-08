// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod r#impl;
mod resource_facade_factory;
mod resource_kind_lookup_service;
mod resource_manifest_discovery_service;
mod resource_manifest_execution_service;
mod resource_summary_service;

use dill::CatalogBuilder;
pub use resource_facade_factory::*;
pub use resource_kind_lookup_service::*;
pub use resource_manifest_discovery_service::*;
pub use resource_manifest_execution_service::*;
pub use resource_summary_service::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder) {
    catalog_builder.add::<r#impl::ResourceFacadeFactoryImpl>();
    catalog_builder.add::<r#impl::ResourceKindLookupServiceImpl>();
    catalog_builder.add::<r#impl::ResourceManifestDiscoveryServiceImpl>();
    catalog_builder.add::<r#impl::ResourceManifestExecutionServiceImpl>();
    catalog_builder.add::<r#impl::ResourceSummaryServiceImpl>();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
