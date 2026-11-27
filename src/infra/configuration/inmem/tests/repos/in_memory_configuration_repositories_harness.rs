// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct InMemoryConfigurationRepositoriesHarness {
    pub catalog: Catalog,
}

impl InMemoryConfigurationRepositoriesHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        kamu_resources_inmem::register_dependencies(&mut catalog_builder);
        kamu_configuration_inmem::register_dependencies(&mut catalog_builder);
        catalog_builder.add::<InMemoryAccountRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
