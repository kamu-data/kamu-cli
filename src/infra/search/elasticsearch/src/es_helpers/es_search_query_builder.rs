// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_search::*;

use crate::es_helpers::{MultiMatchPolicy, PhraseSearchPolicy};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchQueryBuilder {}

impl ElasticsearchQueryBuilder {
    pub fn build_listing_query(
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        sort: &[SearchSortSpec],
        page: &SearchPaginationSpec,
    ) -> serde_json::Value {
        serde_json::json!({
            "query": Self::maybe_filter_argument(filter, Self::all_query()),
            "sort": Self::sort_argument(sort),
            "_source": Self::source_argument(source),
            "from": page.offset,
            "size": page.limit,
        })
    }

    pub fn build_best_fields_search_query(
        textual_prompt: &str,
        operator: FullTextSearchTermOperator,
        tie_breaker: f32,
        text_fields_policy: &MultiMatchPolicy,
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        sort: &[SearchSortSpec],
        page: &SearchPaginationSpec,
        options: &TextSearchOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "query": Self::maybe_filter_argument(filter, serde_json::json!({
                "multi_match": {
                    "query": textual_prompt,
                    "type": "best_fields",
                    "operator": operator.to_string(),
                    "tie_breaker": tie_breaker,
                    "fields": text_fields_policy
                        .specs
                        .iter()
                        .map(|spec| format!("{}^{}", spec.field_name, spec.boost))
                        .collect::<Vec<_>>(),
                }
            })),
            "sort": Self::sort_argument(sort),
            "_source": Self::source_argument(source),
            "from": page.offset,
            "size": page.limit,
        });

        Self::apply_text_search_options(&mut query_json, options);

        query_json
    }

    pub fn build_most_fields_search_query(
        textual_prompt: &str,
        operator: FullTextSearchTermOperator,
        text_fields_policy: &MultiMatchPolicy,
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        sort: &[SearchSortSpec],
        page: &SearchPaginationSpec,
        options: &TextSearchOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "query": Self::maybe_filter_argument(filter, serde_json::json!({
                "multi_match": {
                    "query": textual_prompt,
                    "type": "most_fields",
                    "operator": operator.to_string(),
                    "fields": text_fields_policy
                        .specs
                        .iter()
                        .map(|spec| format!("{}^{}", spec.field_name, spec.boost))
                        .collect::<Vec<_>>(),
                }
            })),
            "sort": Self::sort_argument(sort),
            "_source": Self::source_argument(source),
            "from": page.offset,
            "size": page.limit,
        });

        Self::apply_text_search_options(&mut query_json, options);

        query_json
    }

    pub fn build_phrase_search_query(
        textual_prompt: &str,
        phrase_fields_policy: &PhraseSearchPolicy,
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        page: &SearchPaginationSpec,
        options: &TextSearchOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "query": Self::maybe_filter_argument(filter, serde_json::json!({
                "bool": {
                    "should": phrase_fields_policy.specs.iter().map(|spec| {
                        serde_json::json!({
                            "match_phrase": {
                                spec.field_name.clone(): {
                                    "query": textual_prompt,
                                    "slop": spec.slop,
                                    "boost": spec.boost,
                                }
                            }
                        })
                    }).collect::<Vec<_>>(),
                    "minimum_should_match": 1,
                }
            })),
            "_source": Self::source_argument(source),
            "from": page.offset,
            "size": page.limit,
        });

        Self::apply_text_search_options(&mut query_json, options);

        query_json
    }

    pub fn build_vector_search_query(
        embedding_field: SearchFieldPath,
        prompt_embedding: &[f32],
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        limit: usize,
        options: &VectorSearchOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "track_total_hits": false,
            "knn": Self::knn_argument(embedding_field, prompt_embedding, filter, options.knn),
            "_source": Self::source_argument(source),
            "size": limit,
        });

        if options.enable_explain {
            query_json["explain"] = serde_json::json!(true);
        }

        query_json
    }

    // Unfortunately, paid feature
    /*pub fn build_hybrid_search_query(
        embedding_field: SearchFieldPath,
        textual_prompt: &str,
        prompt_embedding: &[f32],
        filter: Option<&SearchFilterExpr>,
        source: &SearchRequestSourceSpec,
        limit: usize,
        options: &HybridSearchOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
             "track_total_hits": false,
            "_source": Self::source_argument(source),
            "size": limit,
            "retriever": {
                "rrf": {
                    "retrievers": [
                        {
                            "standard": Self::query_argument(Some(textual_prompt), filter)
                        },
                        {
                            "knn": Self::knn_argument(
                                embedding_field,
                                prompt_embedding,
                                filter,
                                options.knn,
                            )
                        }
                    ],
                    "rank_window_size": options.rrf.rank_window_size,
                    "rank_constant": options.rrf.rank_constant,
                }
            }
        });

        if options.enable_explain {
            query_json["explain"] = serde_json::json!(true);
        }

        query_json
    }*/

    fn maybe_filter_argument(
        filter: Option<&SearchFilterExpr>,
        inner_query: serde_json::Value,
    ) -> serde_json::Value {
        if let Some(filter_expr) = filter {
            let filter = Self::filter(filter_expr);
            serde_json::json!({
                "bool": {
                    "must": inner_query,
                    "filter": filter,
                }
            })
        } else {
            inner_query
        }
    }

    fn knn_argument(
        embedding_field: &str,
        prompt_embedding: &[f32],
        filter: Option<&SearchFilterExpr>,
        knn_opts: KnnOptions,
    ) -> serde_json::Value {
        let mut query_json = serde_json::json!({
            "field": format!("{}.embedding", embedding_field),
            "query_vector": prompt_embedding,
            "k": knn_opts.k,
            "num_candidates": knn_opts.num_candidates
        });

        if let Some(filter_expr) = filter {
            let filter_json = Self::filter(filter_expr);
            query_json["filter"] = filter_json;
        }

        query_json
    }

    #[expect(dead_code)]
    fn simple_textual_query(prompt: &str) -> serde_json::Value {
        let query = prompt.trim();
        if !query.is_empty() {
            serde_json::json!({
                "simple_query_string": {
                    "query": query,
                    "default_operator": "or"
                }
            })
        } else {
            serde_json::json!({
                "match_all": {}
            })
        }
    }

    fn all_query() -> serde_json::Value {
        serde_json::json!({
            "match_all": {}
        })
    }

    fn filter(filter_expr: &SearchFilterExpr) -> serde_json::Value {
        match filter_expr {
            SearchFilterExpr::Field { field, op } => match op {
                SearchFilterOp::Eq(value) => {
                    serde_json::json!({
                        "term": {
                            *field: value
                        }
                    })
                }

                SearchFilterOp::Ne(value) => {
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

                SearchFilterOp::Lt(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "lt": value
                            }
                        }
                    })
                }

                SearchFilterOp::Lte(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "lte": value
                            }
                        }
                    })
                }

                SearchFilterOp::Gt(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "gt": value
                            }
                        }
                    })
                }

                SearchFilterOp::Gte(value) => {
                    serde_json::json!({
                        "range": {
                            *field: {
                                "gte": value
                            }
                        }
                    })
                }

                SearchFilterOp::In(values) => {
                    serde_json::json!({
                        "terms": {
                            *field: values
                        }
                    })
                }

                SearchFilterOp::Prefix(prefix) => {
                    serde_json::json!({
                        "prefix": {
                            *field: prefix
                        }
                    })
                }
            },
            SearchFilterExpr::And(operands) => {
                let parts = operands.iter().map(Self::filter).collect::<Vec<_>>();
                serde_json::json!({
                    "bool": {
                        "must": parts
                    },
                })
            }
            SearchFilterExpr::Or(operands) => {
                let parts = operands.iter().map(Self::filter).collect::<Vec<_>>();
                serde_json::json!({
                    "bool": {
                        "should": parts,
                        "minimum_should_match": 1,
                    },
                })
            }
            SearchFilterExpr::Not(operand) => {
                let part = Self::filter(operand);
                serde_json::json!({
                    "bool": {
                        "must_not": part,
                    },
                })
            }
        }
    }

    fn source_argument(source_spec: &kamu_search::SearchRequestSourceSpec) -> serde_json::Value {
        match source_spec {
            SearchRequestSourceSpec::None => serde_json::json!(false),

            SearchRequestSourceSpec::All => serde_json::json!(true),

            SearchRequestSourceSpec::Particular(fields) => {
                serde_json::json!(fields)
            }

            SearchRequestSourceSpec::Complex {
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

    fn sort_argument(sort_specs: &[SearchSortSpec]) -> serde_json::Value {
        fn relevance_sort() -> serde_json::Value {
            serde_json::json!({"_score": {"order": "desc"}})
        }

        let parts = sort_specs
            .iter()
            .map(|sort_part| match sort_part {
                kamu_search::SearchSortSpec::Relevance => relevance_sort(),
                kamu_search::SearchSortSpec::ByField {
                    field,
                    direction,
                    nulls_first,
                } => {
                    serde_json::json!({
                        *field: {
                            "order": match direction {
                                SearchSortDirection::Ascending => "asc",
                                SearchSortDirection::Descending => "desc",
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

    fn highlight_argument(options: &TextSearchOptions) -> Option<serde_json::Value> {
        if options.enable_highlighting {
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

    fn apply_text_search_options(query_json: &mut serde_json::Value, options: &TextSearchOptions) {
        if let Some(highlight_json) = Self::highlight_argument(options) {
            query_json["highlight"] = highlight_json;
        }

        if options.enable_explain {
            query_json["explain"] = serde_json::json!(true);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
