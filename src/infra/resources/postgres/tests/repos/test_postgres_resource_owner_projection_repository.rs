// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use database_common::{PostgresTransactionManager, TransactionRefT};
use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use dill::{Catalog, CatalogBuilder};
use kamu_resources::{
    ResourceHeaders,
    ResourceHeadersExt,
    ResourceID,
    ResourceOwnerProjectionRepository,
    ResourceRepository,
    ResourceSnapshot,
    TypeUri,
};
use kamu_resources_postgres::{
    PostgresResourceOwnerProjectionRepository,
    PostgresResourceRepository,
};
use kamu_resources_repo_tests::resource_owner_projection_repository_test_suite as owner_projection_repo_suite;
use sqlx::{PgPool, Postgres};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = owner_projection_repo_suite::test_replace_owners,
    harness = PostgresResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = postgres,
    fixture = owner_projection_repo_suite::test_rolled_back_replace_keeps_previous_owners,
    harness = PostgresResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(database, postgres)]
#[test_log::test(sqlx::test(migrations = "../../../../migrations/postgres"))]
async fn test_deleting_the_row_of_a_resource_deletes_its_owners(pg_pool: PgPool) {
    let transaction = TransactionRefT::<Postgres>::new(pg_pool);
    let catalog = {
        let mut b = CatalogBuilder::new();
        transaction.clone().into_erased().register(&mut b);
        b.add::<PostgresResourceRepository>();
        b.add::<PostgresResourceOwnerProjectionRepository>();
        b.build()
    };
    let resource_repo = catalog.get_one::<dyn ResourceRepository>().unwrap();
    let owner_repo = catalog
        .get_one::<dyn ResourceOwnerProjectionRepository>()
        .unwrap();

    let id = ResourceID::new(uuid::Uuid::new_v4());
    let snapshot = ResourceSnapshot {
        id,
        schema: TypeUri::new_unchecked("TestKind"),
        headers: ResourceHeaders::simple(
            Utc::now(),
            id,
            odf::ResourceOwnerHandle::new_test("test-account"),
            "owned",
        ),
        spec: serde_json::json!({}),
        status: None,
        last_event_id: None,
    };
    resource_repo.create_resource(&snapshot).await.unwrap();

    let owner_id = ResourceID::new(uuid::Uuid::new_v4());
    owner_repo.replace_owners(&id, &[owner_id]).await.unwrap();

    {
        let mut tr = transaction.lock().await;
        let connection_mut = tr.connection_mut().await.unwrap();
        sqlx::query("DELETE FROM resources WHERE resource_id = $1")
            .bind(*id.as_ref())
            .execute(connection_mut)
            .await
            .unwrap();
    }

    assert_eq!(owner_repo.find_owners(&id).await.unwrap(), []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresResourceOwnerProjectionRepositoryHarness {
    catalog: Catalog,
}

impl PostgresResourceOwnerProjectionRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceOwnerProjectionRepository>();
        catalog_builder.add::<PostgresResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
