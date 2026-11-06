// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::InMemoryTransactionManager;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Every in-memory dataset repository, with the account repository their
/// suites create owners in
pub(crate) struct InMemoryDatasetRepositoriesHarness {
    pub catalog: Catalog,
}

impl InMemoryDatasetRepositoriesHarness {
    /// Changes apply at once, as no transaction is tracked
    pub fn new() -> Self {
        Self {
            catalog: Self::catalog_builder().build(),
        }
    }

    /// Changes collect in each transaction's working set until it commits
    pub fn new_transactional() -> Self {
        let mut catalog_builder = Self::catalog_builder();
        catalog_builder.add::<InMemoryTransactionManager>();
        Self {
            catalog: catalog_builder.build(),
        }
    }

    fn catalog_builder() -> CatalogBuilder {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryAccountRepository>();
        kamu_datasets_inmem::register_dependencies(&mut catalog_builder);
        catalog_builder
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
