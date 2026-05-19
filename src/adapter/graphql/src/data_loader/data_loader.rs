// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::sync::Arc;

use async_graphql::dataloader::DataLoader;
use internal_error::InternalError;
use tracing::Instrument;

use crate::data_loader::{AccountEntityLoader, DatasetHandleLoader};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccountEntityDataLoader = DataLoader<AccountEntityLoader>;
pub type DatasetHandleDataLoader = DataLoader<DatasetHandleLoader>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Spawner that propagates tracing context to spawned tasks
fn tracing_spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let span = tracing::Span::current();
    tokio::spawn(async move { f.instrument(span).await })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn account_entity_data_loader(catalog: &dill::Catalog) -> AccountEntityDataLoader {
    DataLoader::new(AccountEntityLoader::new(catalog.weak_ref()), tracing_spawn)
}

pub fn dataset_handle_data_loader(catalog: &dill::Catalog) -> DatasetHandleDataLoader {
    DataLoader::new(DatasetHandleLoader::new(catalog.weak_ref()), tracing_spawn)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn data_loader_error_mapper(e: Arc<InternalError>) -> InternalError {
    InternalError::new(e)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
