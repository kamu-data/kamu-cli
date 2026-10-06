// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_flow_system_postgres::PostgresFlowActivationLinkRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_no_links_initially,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture =
        kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_save_and_get_link,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_save_link_is_idempotent,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_one_upstream_activates_many_downstreams,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_many_upstreams_activate_one_downstream,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_flow_system_repo_tests::test_flow_activation_link_repository::test_get_links_of_several_upstreams,
    harness = PostgresFlowActivationLinkHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresFlowActivationLinkHarness {
    catalog: Catalog,
}

impl PostgresFlowActivationLinkHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresFlowActivationLinkRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
