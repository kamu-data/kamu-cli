// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::KamuBackgroundCatalog;
use kamu_search::SearchIndexer;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AdminSearchMut;

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
enum SearchEntityName {
    Datasets,
    Accounts,
}

impl SearchEntityName {
    fn as_schema_name(self) -> &'static str {
        match self {
            Self::Datasets => kamu_datasets::dataset_search_schema::SCHEMA_NAME,
            Self::Accounts => kamu_accounts::account_search_schema::SCHEMA_NAME,
        }
    }
}

#[Object]
impl AdminSearchMut {
    async fn reset_search_indices(
        &self,
        ctx: &Context<'_>,
        entity_names: Option<Vec<SearchEntityName>>,
    ) -> Result<String> {
        let background_catalog = from_catalog_n!(ctx, KamuBackgroundCatalog);
        let system_user_catalog = background_catalog.system_user_catalog();

        let search_indexer = system_user_catalog.get_one::<dyn SearchIndexer>().unwrap();
        let entity_schema_names = entity_names
            .unwrap_or_default()
            .into_iter()
            .map(SearchEntityName::as_schema_name)
            .map(ToOwned::to_owned)
            .collect();

        search_indexer
            .reset_search_indices(entity_schema_names)
            .await?;
        Ok("Ok".to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
