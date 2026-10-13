// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system_inmem::InMemoryFlowActivationLinkRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_no_links_initially,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_save_and_get_link,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_save_link_is_idempotent,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_one_upstream_activates_many_downstreams,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_many_upstreams_activate_one_downstream,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_get_links_of_several_upstreams,
    harness = InMemoryFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryFlowActivationLinkHarness {
    catalog: Catalog,
}

impl InMemoryFlowActivationLinkHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryFlowActivationLinkRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
