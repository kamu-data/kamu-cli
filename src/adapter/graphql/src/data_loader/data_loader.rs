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
use kamu_auth_rebac::RebacDatasetRegistryFacade;

use crate::data_loader::{AccountEntityLoader, DatasetHandleLoader};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccountEntityDataLoader = DataLoader<AccountEntityLoader>;
pub type DatasetHandleDataLoader = DataLoader<DatasetHandleLoader>;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Public only for tests
pub fn account_entity_data_loader(catalog: &dill::Catalog) -> AccountEntityDataLoader {
    let account_service = catalog.get_one::<dyn AccountService>().unwrap();

    DataLoader::new(AccountEntityLoader::new(account_service), tokio::spawn)
}

// Public only for tests
pub fn dataset_handle_data_loader(catalog: &dill::Catalog) -> DatasetHandleDataLoader {
    let rebac_dataset_registry_facade =
        catalog.get_one::<dyn RebacDatasetRegistryFacade>().unwrap();

    DataLoader::new(
        DatasetHandleLoader::new(rebac_dataset_registry_facade),
        tokio::spawn,
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[expect(clippy::needless_pass_by_value)]
pub fn data_loader_error_mapper(e: Arc<InternalError>) -> InternalError {
    e.reason().int_err()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
