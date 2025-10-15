// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use async_graphql::dataloader::DataLoader;
use internal_error::{ErrorIntoInternal, InternalError};
use kamu_accounts::AccountService;
use kamu_datasets::DatasetEntryService;

use crate::data_loader::EntityLoader;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type EntityDataLoader = DataLoader<EntityLoader>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn data_loader(catalog: &dill::Catalog) -> EntityDataLoader {
    let account_service = catalog.get_one::<dyn AccountService>().unwrap();
    let dataset_entry_service = catalog.get_one::<dyn DatasetEntryService>().unwrap();

    DataLoader::new(
        EntityLoader::new(account_service, dataset_entry_service),
        tokio::spawn,
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn data_loader_error_mapper(e: Arc<InternalError>) -> InternalError {
    e.reason().int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
