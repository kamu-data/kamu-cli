// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::CurrentAccountSubject;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct KamuBackgroundCatalog {
    base_catalog: dill::Catalog,
    system_user_subject: CurrentAccountSubject,
}

impl KamuBackgroundCatalog {
    pub fn new(base_catalog: dill::Catalog, system_user_subject: CurrentAccountSubject) -> Self {
        Self {
            base_catalog,
            system_user_subject,
        }
    }

    #[inline]
    pub fn base_catalog(&self) -> &dill::Catalog {
        &self.base_catalog
    }

    pub fn system_user_catalog(&self) -> dill::Catalog {
        let mut b = dill::CatalogBuilder::new_chained(&self.base_catalog);
        b.add_value(self.system_user_subject.clone());
        b.add_value(self.clone());
        b.build()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
