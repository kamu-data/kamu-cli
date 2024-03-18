// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseCatalogInitializer, DatabaseConfiguration};
use dill::{CatalogBuilder, Component};
use kamu_core::{InternalError, ResultIntoInternal};

use crate::{PostgresAccountRepository, PostgresConnectionPool};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct PostgresCatalogInitializer {}

impl DatabaseCatalogInitializer for PostgresCatalogInitializer {
    fn init_database_components(
        &self,
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), InternalError> {
        let pg_pool = PostgresConnectionPool::build_pg_pool(db_configuration).int_err()?;

        catalog_builder.add_builder(PostgresConnectionPool::builder().with_pg_pool(pg_pool));
        catalog_builder.add::<PostgresAccountRepository>();

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
