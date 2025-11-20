// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{FullTextSearchRequest, FullTextSearchRequestSourceSpec, FullTextSortDirection};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchQueryBuilder {}

impl ElasticSearchQueryBuilder {
    pub fn build_search_query(request: &FullTextSearchRequest) -> serde_json::Value {
        let textual_query = Self::textual_query(request);
        serde_json::json!({
            "query": textual_query,
            "sort": Self::sort_argument(request),
            "_source": Self::source_argument(request),
            "from": request.page.offset,
            "size": request.page.limit,
        })
    }

    fn textual_query(request: &FullTextSearchRequest) -> serde_json::Value {
        let query = request.query.as_ref().map(|q| q.trim());
        if let Some(query) = query {
            serde_json::json!({
                "simple_query_string": {
                    "query": query,
                    "default_operator": "and"
                }
            })
        } else {
            serde_json::json!({
                "match_all": {}
            })
        }
    }

    fn source_argument(request: &kamu_search::FullTextSearchRequest) -> serde_json::Value {
        match &request.source {
            FullTextSearchRequestSourceSpec::None => serde_json::json!(false),

            FullTextSearchRequestSourceSpec::All => serde_json::json!(true),

            FullTextSearchRequestSourceSpec::Particular(fields) => {
                serde_json::json!(fields)
            }

            FullTextSearchRequestSourceSpec::Complex {
                include_patterns,
                exclude_patterns,
            } => {
                serde_json::json!({
                    "include": include_patterns,
                    "exclude": exclude_patterns,
                })
            }
        }
    }

    fn sort_argument(req: &FullTextSearchRequest) -> serde_json::Value {
        fn relevance_sort() -> serde_json::Value {
            serde_json::json!([{"_score": {"order": "desc"}}])
        }

        let parts = req
            .sort
            .iter()
            .map(|sort_part| match sort_part {
                kamu_search::FullTextSortSpec::Relevance => relevance_sort(),
                kamu_search::FullTextSortSpec::ByField {
                    field,
                    direction,
                    nulls_first,
                } => {
                    serde_json::json!({
                        *field: {
                            "order": match direction {
                                FullTextSortDirection::Ascending => "asc",
                                FullTextSortDirection::Descending => "desc",
                            },
                            "missing": if *nulls_first { "first" } else { "last" },
                            "unmapped_type": "keyword",
                        }
                    })
                }
            })
            .collect::<Vec<_>>();

        if parts.is_empty() {
            relevance_sort()
        } else {
            serde_json::Value::Array(parts)
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
