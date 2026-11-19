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

use crate::DatabaseTransactionManager;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Runs `f` in a transaction of the catalog's `DatabaseTransactionManager` and
/// rolls it back, so a test can check that nothing `f` wrote is left
pub async fn in_rolled_back_transaction<F, Fut>(catalog: &Catalog, f: F)
where
    F: FnOnce(Catalog) -> Fut,
    Fut: Future<Output = ()>,
{
    let transaction_manager = catalog.get_one::<dyn DatabaseTransactionManager>().unwrap();
    let transaction = transaction_manager.make_transaction_ref().await.unwrap();

    let mut b = catalog.builder_chained();
    transaction.register(&mut b);
    f(b.build()).await;

    transaction_manager
        .rollback_transaction(transaction)
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
