// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{DatabaseConfiguration, DatabaseError};
use dill::*;
use sqlx::MySqlPool;

use crate::{MySqlAccountRepository, MySqlTransactionManager};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPlugin {}

#[component(pub)]
impl MySqlPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), DatabaseError> {
        let mysql_pool = Self::open_mysql_pool(db_configuration)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();
        catalog_builder.add::<MySqlAccountRepository>();

        Ok(())
    }

    fn open_mysql_pool(
        db_configuration: &DatabaseConfiguration,
    ) -> Result<MySqlPool, DatabaseError> {
        MySqlPool::connect_lazy(db_configuration.connection_string().as_str())
            .map_err(DatabaseError::SqlxError)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
