// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::CatalogBuilder;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// TODO: Private Datasets: use brand-new enum for workspace tenantness
pub fn register_dependencies(catalog_builder: &mut CatalogBuilder, multi_tenant_workspace: bool) {
    catalog_builder.add::<RebacIndexer>();
    catalog_builder.add::<RebacServiceImpl>();

    if multi_tenant_workspace {
        catalog_builder.add::<MultiTenantRebacDatasetLifecycleMessageConsumer>();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
