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

use crate::{MySQLAccountRepository, MySQLConnectionPool};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct MySQLCatalogInitializer {}

impl DatabaseCatalogInitializer for MySQLCatalogInitializer {
    fn init_database_components(
        &self,
        catalog_builder: &mut CatalogBuilder,
        db_configuration: &DatabaseConfiguration,
    ) -> Result<(), InternalError> {
        let mysql_pool = MySQLConnectionPool::build_mysql_pool(db_configuration).int_err()?;

        catalog_builder.add_builder(MySQLConnectionPool::builder().with_mysql_pool(mysql_pool));
        catalog_builder.add::<MySQLAccountRepository>();

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
