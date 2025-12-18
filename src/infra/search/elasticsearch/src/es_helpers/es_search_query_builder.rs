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

pub struct ElasticSearchQueryBuilder {}

impl ElasticSearchQueryBuilder {
    pub fn build_search_query(request: &FullTextSearchRequest) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "query": Self::query_argument(request),
            "sort": Self::sort_argument(request),
            "_source": Self::source_argument(request),
            "from": request.page.offset,
            "size": request.page.limit,
        });

        if let Some(highlight_json) = Self::highlight_argument(request) {
            query_json["highlight"] = highlight_json;
        }

        if request.options.enable_explain {
            query_json["explain"] = serde_json::json!(true);
        }

        query_json
    }

    fn query_argument(request: &FullTextSearchRequest) -> serde_json::Value {
        let textual_query = Self::textual_query(request);
        if let Some(filter_expr) = &request.filter {
            let filter = Self::filter(filter_expr);
            serde_json::json!({
                "bool": {
                    "must": textual_query,
                    "filter": filter,
                }
            })
        } else {
            textual_query
        }
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

    fn filter(filter_expr: &FullTextSearchFilterExpr) -> serde_json::Value {
        match filter_expr {
            FullTextSearchFilterExpr::Field { field, op } => match op {
                FullTextSearchFilterOp::Eq(value) => {
                    serde_json::json!({
                        "term": {
                            *field: value
                        }
                    })
                }

                FullTextSearchFilterOp::Ne(value) => {
                    serde_json::json!({
                        "bool": {
                            "must_not": {
                                "term": {
                                    *field: value
                                }
                            }
                        }
                    })
                }

                FullTextSearchFilterOp::Lt(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "lt": value
                            }
                        }
                    })
                }

                FullTextSearchFilterOp::Lte(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "lte": value
                            }
                        }
                    })
                }

                FullTextSearchFilterOp::Gt(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "gt": value
                            }
                        }
                    })
                }

                FullTextSearchFilterOp::Gte(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "gte": value
                            }
                        }
                    })
                }

                FullTextSearchFilterOp::In(values) => {
                    serde_json::json!({
                        "terms": {
                            *field: values
                        }
                    })
                }

                FullTextSearchFilterOp::Prefix(prefix) => {
                    serde_json::json!({
                        "prefix": {
                            *field: prefix
                        }
                    })
                }
            },
            FullTextSearchFilterExpr::And(operands) => {
                let parts = operands.iter().map(Self::filter).collect::<Vec<_>>();
                serde_json::json!({
                    "bool": {
                        "must": parts
                    },
                })
            }
            FullTextSearchFilterExpr::Or(operands) => {
                let parts = operands.iter().map(Self::filter).collect::<Vec<_>>();
                serde_json::json!({
                    "bool": {
                        "should": parts,
                        "minimum_should_match": 1,
                    },
                })
            }
            FullTextSearchFilterExpr::Not(operand) => {
                let part = Self::filter(operand);
                serde_json::json!({
                    "bool": {
                        "must_not": part,
                    },
                })
            }
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
            serde_json::json!({"_score": {"order": "desc"}})
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
                            "missing": if *nulls_first { "_first" } else { "_last" },
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

    fn highlight_argument(request: &FullTextSearchRequest) -> Option<serde_json::Value> {
        if request.options.enable_highlighting {
            Some(serde_json::json!({
                "pre_tags": ["<em>"],
                "post_tags": ["</em>"],
                "fields": {
                    "*": { "fragment_size": 100, "number_of_fragments": 1 }
                }
            }))
        } else {
            None
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
