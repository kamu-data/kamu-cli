// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::Principal;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct KamuBackgroundCatalog {
    base_catalog: dill::Catalog,
}

impl KamuBackgroundCatalog {
    pub fn new(base_catalog: dill::Catalog) -> Self {
        Self { base_catalog }
    }

    #[inline]
    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    /// A catalog of system work acting as [`Principal::Node`]
    pub fn node_catalog(&self) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(&self.base_catalog);
        b.add_value(Principal::Node);
        b.add_value(self.clone());
        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
