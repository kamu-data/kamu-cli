// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use sqlx::SqlitePool;

use crate::{DatabaseTransactionManager, TransactionRef};

///////////////////////////////////////////////////////////////////////////////

pub struct FakeDatabaseTransactionManager {
    // Since the pool will never be connected, an SqlitePool is used for simplicity
    never_connected_pool: SqlitePool,
}

impl FakeDatabaseTransactionManager {
    pub fn new() -> Self {
        Self {
            never_connected_pool: SqlitePool::connect_lazy("").unwrap(),
        }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for FakeDatabaseTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        Ok(TransactionRef::new(self.never_connected_pool.clone()))
    }

    async fn commit_transaction(
        &self,
        _transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        Ok(())
    }

    async fn rollback_transaction(
        &self,
        _transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////
