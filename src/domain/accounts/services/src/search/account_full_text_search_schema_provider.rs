// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct AccountFullTextSearchSchemaProvider {}

#[async_trait::async_trait]
impl kamu_search::FullTextSearchEntitySchemaProvider for AccountFullTextSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.accounts.AccountFullTextSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::FullTextSearchEntitySchema] {
        &[ACCOUNT_FULL_TEXT_SEARCH_ENTITY_SCHEMA]
    }

    async fn run_schema_initial_indexing(
        &self,
        _repo: &dyn FullTextSearchRepository,
        _schema: &FullTextSearchEntitySchema,
    ) -> Result<usize, InternalError> {
        // TODO
        Ok(0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT: &str = "kamu-accounts";
const FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const ACCOUNT_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: "account_name",
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
    FullTextSchemaField {
        path: "display_name",
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
];

const ACCOUNT_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_ACCOUNT_VERSION,
        fields: ACCOUNT_FIELDS,
        upgrade_mode: FullTextSearchEntitySchemaUpgradeMode::Reindex,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
