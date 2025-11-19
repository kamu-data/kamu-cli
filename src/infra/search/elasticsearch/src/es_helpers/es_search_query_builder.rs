// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::FullTextSearchRequest;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticSearchQueryBuilder {}

impl ElasticSearchQueryBuilder {
    pub fn build_search_query(request: &FullTextSearchRequest) -> serde_json::Value {
        let query = request.query.as_ref().map(|q| q.trim());
        if let Some(query) = query {
            serde_json::json!({
                "query": {
                    "query_string": {
                        "query": query,
                    }
                }
            })
        } else {
            serde_json::json!({
                "query": {
                    "match_all": {}
                }
            })
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
