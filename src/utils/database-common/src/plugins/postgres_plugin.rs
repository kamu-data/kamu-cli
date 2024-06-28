// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use secrecy::{ExposeSecret, Secret};
use sqlx::PgPool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresPlugin {}

#[component(pub)]
impl PostgresPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(catalog_builder: &mut CatalogBuilder) {
        catalog_builder.add::<Self>();
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresPasswordRefresher>();
    }

    pub fn catalog_with_connected_pool(
        base_catalog: &Catalog,
        connection_string: &Secret<String>,
    ) -> Result<Catalog, DatabaseError> {
        let pg_pool = Self::open_pg_pool(connection_string)?;

        Ok(CatalogBuilder::new_chained(base_catalog)
            .add_value(pg_pool)
            .build())
    }

    #[tracing::instrument(level = "info", skip_all)]
    fn open_pg_pool(connection_string: &Secret<String>) -> Result<PgPool, DatabaseError> {
        PgPool::connect_lazy(connection_string.expose_secret()).map_err(DatabaseError::SqlxError)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
