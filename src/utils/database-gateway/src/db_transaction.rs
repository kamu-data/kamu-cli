// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use thiserror::Error;

/////////////////////////////////////////////////////////////////////////////////////////

/*#[async_trait::async_trait]
pub trait DatabaseTransaction {
    /// Commits transaction
    async fn commit(mut self) -> Result<(), DatabaseTransactionError>;

    /// Rollbacks transaction
    async fn rollback(mut self) -> Result<(), DatabaseTransactionError>;
}*/

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum DatabaseTransactionError {
    #[error(transparent)]
    SqlxError(sqlx::Error),
}

/////////////////////////////////////////////////////////////////////////////////////////
