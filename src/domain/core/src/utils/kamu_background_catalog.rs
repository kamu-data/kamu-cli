// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct KamuBackgroundCatalog {
    background_catalog: dill::Catalog,
}

impl KamuBackgroundCatalog {
    pub fn new(background_catalog: dill::Catalog) -> Self {
        Self { background_catalog }
    }

    #[inline]
    pub fn catalog(&self) -> &dill::Catalog {
        &self.background_catalog
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
