// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;
use kamu_core::TenancyConfig;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn register_dependencies(catalog_builder: &mut CatalogBuilder, tenancy_config: TenancyConfig) {
    catalog_builder.add::<RebacIndexer>();
    catalog_builder.add::<RebacServiceImpl>();

    if tenancy_config == TenancyConfig::MultiTenant {
        catalog_builder.add::<MultiTenantRebacDatasetLifecycleMessageConsumer>();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////