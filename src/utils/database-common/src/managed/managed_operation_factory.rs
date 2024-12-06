// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;

use dill::{Catalog, CatalogBuilder};
use internal_error::InternalError;

use super::ManagedOperationRef;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManagedOperationFactory: Sync + Send {
    async fn create_operation(&self) -> ManagedOperationRef;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn run_with_managed_operation<F, RF, RT, RE>(
    f: F,
    base_catalog: &Catalog,
) -> Result<RT, RE>
where
    F: FnOnce(Catalog) -> RF,
    RF: Future<Output = Result<RT, RE>>,
    RE: From<InternalError>,
{
    let managed_operation_ref = base_catalog
        .get_one::<dyn ManagedOperationFactory>()
        .unwrap()
        .create_operation()
        .await;
    let operation_catalog = CatalogBuilder::new_chained(base_catalog)
        .add_value(managed_operation_ref.clone())
        .build();

    match f(operation_catalog).await {
        Ok(res) => match managed_operation_ref.do_commit().await {
            Ok(_) => Ok(res),
            Err(e) => Err(e.into()),
        },
        Err(e) => Err(e),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
