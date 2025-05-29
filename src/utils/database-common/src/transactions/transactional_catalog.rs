// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use dill::{Catalog, component};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A wrapper structure for the catalog, for a visual understanding of what we
/// will be inside the transaction
#[derive(Clone)]
pub struct TransactionalCatalog(Catalog);

#[component(pub)]
impl TransactionalCatalog {
    pub fn new(catalog: Catalog) -> Self {
        Self(catalog)
    }

    pub fn into_inner(self) -> Catalog {
        self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl Deref for TransactionalCatalog {
    type Target = Catalog;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
