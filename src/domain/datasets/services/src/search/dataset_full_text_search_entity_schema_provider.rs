// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn kamu_search::FullTextSearchEntitySchemaProvider)]
pub struct DatasetFullTextSearchSchemaProvider {}

impl kamu_search::FullTextSearchEntitySchemaProvider for DatasetFullTextSearchSchemaProvider {
    fn provider_name(&self) -> &'static str {
        "dev.kamu.domain.datasets.DatasetFullTextSearchSchemaProvider"
    }

    fn provide_schemas(&self) -> &[kamu_search::FullTextSearchEntitySchema] {
        &[DATASET_FULL_TEXT_SEARCH_ENTITY_SCHEMA]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET: &str = "kamu-datasets";
const FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION: u32 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_FIELDS: &[FullTextSchemaField] = &[
    FullTextSchemaField {
        path: "dataset_name",
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
    FullTextSchemaField {
        path: "alias",
        kind: FullTextSchemaFieldKind::Text,
        searchable: true,
        sortable: true,
        filterable: false,
    },
    FullTextSchemaField {
        path: "kind",
        kind: FullTextSchemaFieldKind::Keyword,
        searchable: false,
        sortable: false,
        filterable: true,
    },
];

const DATASET_FULL_TEXT_SEARCH_ENTITY_SCHEMA: FullTextSearchEntitySchema =
    FullTextSearchEntitySchema {
        entity_kind: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET,
        version: FULL_TEXT_SEARCH_ENTITY_KAMU_DATASET_VERSION,
        fields: DATASET_FIELDS,
    };

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
