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
use kamu_auth_rebac_postgres::PostgresRebacRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_try_get_properties_from_nonexistent_entity,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_set_property,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_try_delete_property_from_nonexistent_entity,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_try_delete_nonexistent_property_from_entity,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_delete_property_from_entity,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_delete_entity_properties,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_try_insert_duplicate_entities_relation,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_try_insert_another_entities_relation,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_delete_entities_relation,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_get_relations_crossover_test,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_delete_subject_entities_object_entity_relations,
    harness = PostgresRebacRepositoryHarness
);
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_auth_rebac_repo_tests::test_get_object_entity_relations_matrix,
    harness = PostgresRebacRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresRebacRepositoryHarness {
    catalog: Catalog,
}

impl PostgresRebacRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresRebacRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
