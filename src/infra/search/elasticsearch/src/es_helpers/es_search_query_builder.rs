// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::{FullTextSearchRequest, FullTextSearchRequestSourceFields};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchQueryBuilder {}

impl ElasticSearchQueryBuilder {
    pub fn build_search_query(request: &FullTextSearchRequest) -> serde_json::Value {
        let textual_query = Self::textual_query(request);
        serde_json::json!({
            "query": textual_query,
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
        match &request.source_fields {
            FullTextSearchRequestSourceFields::None => serde_json::json!(false),

            FullTextSearchRequestSourceFields::All => serde_json::json!(true),

            FullTextSearchRequestSourceFields::Particular(fields) => {
                serde_json::json!(fields)
            }

            FullTextSearchRequestSourceFields::Complex {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
