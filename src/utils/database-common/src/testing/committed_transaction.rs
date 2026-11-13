// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use dill::Catalog;
use internal_error::InternalError;

use crate::DatabaseTransactionRunner;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs `f` in a transaction of the catalog's `DatabaseTransactionManager` and
/// commits it, for test setup and checks outside the transactions under test
pub async fn in_committed_transaction<F, Fut, R>(catalog: &Catalog, f: F) -> R
where
    F: FnOnce(Catalog) -> Fut,
    Fut: Future<Output = R>,
{
    DatabaseTransactionRunner::from(catalog.clone())
        .transactional(|catalog| async move { Ok::<_, InternalError>(f(catalog).await) })
        .await
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
