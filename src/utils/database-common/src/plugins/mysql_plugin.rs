// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use secrecy::Secret;
use sqlx::MySqlPool;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct MySqlPlugin {}

#[component(pub)]
impl MySqlPlugin {
    pub fn new() -> Self {
        Self {}
    }

    pub fn init_database_components(
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
        db_password: Option<&Secret<String>>,
    ) -> Result<(), DatabaseError> {
        let mysql_pool = Self::open_mysql_pool(db_configuration, db_password)?;

        catalog_builder.add::<Self>();
        catalog_builder.add_value(mysql_pool);
        catalog_builder.add::<MySqlTransactionManager>();

        Ok(())
    }

    fn open_mysql_pool(
        db_configuration: &DatabaseConfiguration,
        db_password: Option<&Secret<String>>,
    ) -> Result<MySqlPool, DatabaseError> {
        let connection_string = db_configuration.connection_string(db_password);
        MySqlPool::connect_lazy(&connection_string).map_err(DatabaseError::SqlxError)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
