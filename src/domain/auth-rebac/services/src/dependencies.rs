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

pub fn register_dependencies(b: &mut CatalogBuilder, needs_indexing: bool) {
    if needs_indexing {
        b.add::<RebacIndexer>();
    }
    b.add::<RebacDatasetLifecycleMessageConsumer>();
    b.add::<RebacServiceImpl>();
    b.add::<RebacDatasetRegistryFacadeImpl>();
    b.add_value(DefaultAccountProperties {
        is_admin: false,
        can_provision_accounts: false,
    });
    b.add_value(DefaultDatasetProperties {
        allows_anonymous_read: false,
        allows_public_read: false,
    });
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
