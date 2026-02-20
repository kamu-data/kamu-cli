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

#[cfg(test)]
mod tests {
    use indoc::indoc;
    use kamu_search::*;
    use pretty_assertions::assert_eq;

    use super::*;

    fn assert_query_json(actual: &serde_json::Value, expected_json: &str) {
        let expected: serde_json::Value = serde_json::from_str(expected_json).unwrap();
        assert_eq!(expected, *actual);
    }

    #[test]
    fn test_build_listing_query_with_nested_filter_and_default_sort() {
        let filter = SearchFilterExpr::And(vec![
            SearchFilterExpr::Field {
                field: "visibility",
                op: SearchFilterOp::Eq(serde_json::json!("public")),
            },
            SearchFilterExpr::Not(Box::new(SearchFilterExpr::Field {
                field: "is_banned",
                op: SearchFilterOp::Eq(serde_json::json!(true)),
            })),
            SearchFilterExpr::Field {
                field: "status",
                op: SearchFilterOp::Ne(serde_json::json!("deleted")),
            },
            SearchFilterExpr::Field {
                field: "created_at",
                op: SearchFilterOp::Lt(serde_json::json!(1_700_000_000)),
            },
            SearchFilterExpr::Field {
                field: "updated_at",
                op: SearchFilterOp::Lte(serde_json::json!(1_700_000_001)),
            },
            SearchFilterExpr::Field {
                field: "rank",
                op: SearchFilterOp::Gt(serde_json::json!(5)),
            },
            SearchFilterExpr::Field {
                field: "rating",
                op: SearchFilterOp::Gte(serde_json::json!(4.5)),
            },
            SearchFilterExpr::Field {
                field: "kind",
                op: SearchFilterOp::In(vec![
                    serde_json::json!("dataset"),
                    serde_json::json!("flow"),
                ]),
            },
            SearchFilterExpr::Field {
                field: "namespace",
                op: SearchFilterOp::Prefix("org.acme".to_string()),
            },
            SearchFilterExpr::Or(vec![
                SearchFilterExpr::Field {
                    field: "owner",
                    op: SearchFilterOp::Eq(serde_json::json!("alice")),
                },
                SearchFilterExpr::Field {
                    field: "owner",
                    op: SearchFilterOp::Eq(serde_json::json!("bob")),
                },
            ]),
        ]);

        let query = ElasticsearchQueryBuilder::build_listing_query(
            Some(&filter),
            &SearchRequestSourceSpec::Particular(vec!["name", "visibility"]),
            &[],
            &SearchPaginationSpec {
                limit: 5,
                offset: 10,
            },
        );

        assert_query_json(
            &query,
            indoc!(
                r#"
                {
                    "query": {
                        "bool": {
                            "must": {
                                "match_all": {}
                            },
                            "filter": {
                                "bool": {
                                    "must": [
                                        { "term": { "visibility": "public" } },
                                        { "bool": { "must_not": { "term": { "is_banned": true } } } },
                                        { "bool": { "must_not": { "term": { "status": "deleted" } } } },
                                        { "range": { "created_at": { "lt": 1700000000 } } },
                                        { "range": { "updated_at": { "lte": 1700000001 } } },
                                        { "range": { "rank": { "gt": 5 } } },
                                        { "range": { "rating": { "gte": 4.5 } } },
                                        { "terms": { "kind": ["dataset", "flow"] } },
                                        { "prefix": { "namespace": "org.acme" } },
                                        {
                                            "bool": {
                                                "should": [
                                                    { "term": { "owner": "alice" } },
                                                    { "term": { "owner": "bob" } }
                                                ],
                                                "minimum_should_match": 1
                                            }
                                        }
                                    ]
                                }
                            }
                        }
                    },
                    "sort": { "_score": { "order": "desc" } },
                    "_source": ["name", "visibility"],
                    "from": 10,
                    "size": 5
                }
                "#
            ),
        );
    }

    #[test]
    fn test_build_best_fields_query_applies_highlight_and_explain() {
        let policy = MultiMatchPolicy {
            specs: vec![
                crate::es_helpers::MultiMatchFieldSpec {
                    field_name: "name".to_string(),
                    boost: 6.0,
                },
                crate::es_helpers::MultiMatchFieldSpec {
                    field_name: "description".to_string(),
                    boost: 3.5,
                },
            ],
        };
        let sort = [SearchSortSpec::ByField {
            field: "created_at",
            direction: SearchSortDirection::Descending,
            nulls_first: false,
        }];
        let options = TextSearchOptions {
            enable_explain: true,
            enable_highlighting: true,
            boosting_overrides: Default::default(),
        };

        let query = ElasticsearchQueryBuilder::build_best_fields_search_query(
            "wind power",
            FullTextSearchTermOperator::And,
            0.2,
            &policy,
            None,
            &SearchRequestSourceSpec::All,
            &sort,
            &SearchPaginationSpec {
                limit: 20,
                offset: 0,
            },
            &options,
        );

        assert_query_json(
            &query,
            indoc!(
                r#"
                {
                    "query": {
                        "multi_match": {
                            "query": "wind power",
                            "type": "best_fields",
                            "operator": "and",
                            "tie_breaker": 0.2,
                            "fields": ["name^6", "description^3.5"]
                        }
                    },
                    "sort": [
                        {
                            "created_at": {
                                "order": "desc",
                                "missing": "_last",
                                "unmapped_type": "keyword"
                            }
                        }
                    ],
                    "_source": true,
                    "from": 0,
                    "size": 20,
                    "highlight": {
                        "pre_tags": ["<em>"],
                        "post_tags": ["</em>"],
                        "fields": {
                            "*": {
                                "fragment_size": 100,
                                "number_of_fragments": 1
                            }
                        }
                    },
                    "explain": true
                }
                "#
            ),
        );
    }

    #[test]
    fn test_build_phrase_query_with_filter_and_source_complex() {
        let phrase_policy = PhraseSearchPolicy {
            specs: vec![
                crate::es_helpers::PhraseSearchFieldSpec {
                    field_name: "name".to_string(),
                    boost: 8.0,
                    slop: 1,
                },
                crate::es_helpers::PhraseSearchFieldSpec {
                    field_name: "description".to_string(),
                    boost: 4.0,
                    slop: 2,
                },
            ],
        };
        let filter = SearchFilterExpr::Or(vec![
            SearchFilterExpr::Field {
                field: "kind",
                op: SearchFilterOp::Eq(serde_json::json!("dataset")),
            },
            SearchFilterExpr::Field {
                field: "kind",
                op: SearchFilterOp::Eq(serde_json::json!("flow")),
            },
        ]);

        let query = ElasticsearchQueryBuilder::build_phrase_search_query(
            "public data",
            &phrase_policy,
            Some(&filter),
            &SearchRequestSourceSpec::Complex {
                include_patterns: vec!["name*".to_string()],
                exclude_patterns: vec!["secret*".to_string()],
            },
            &SearchPaginationSpec {
                limit: 7,
                offset: 3,
            },
            &TextSearchOptions::default(),
        );

        assert_query_json(
            &query,
            indoc!(
                r#"
                {
                    "query": {
                        "bool": {
                            "must": {
                                "bool": {
                                    "should": [
                                        {
                                            "match_phrase": {
                                                "name": {
                                                    "query": "public data",
                                                    "slop": 1,
                                                    "boost": 8.0
                                                }
                                            }
                                        },
                                        {
                                            "match_phrase": {
                                                "description": {
                                                    "query": "public data",
                                                    "slop": 2,
                                                    "boost": 4.0
                                                }
                                            }
                                        }
                                    ],
                                    "minimum_should_match": 1
                                }
                            },
                            "filter": {
                                "bool": {
                                    "should": [
                                        { "term": { "kind": "dataset" } },
                                        { "term": { "kind": "flow" } }
                                    ],
                                    "minimum_should_match": 1
                                }
                            }
                        }
                    },
                    "_source": {
                        "include": ["name*"],
                        "exclude": ["secret*"]
                    },
                    "from": 3,
                    "size": 7
                }
                "#
            ),
        );
    }

    #[test]
    fn test_build_vector_query_with_filter_and_explain() {
        let filter = SearchFilterExpr::Field {
            field: "visibility",
            op: SearchFilterOp::In(vec![serde_json::json!("public")]),
        };

        let query = ElasticsearchQueryBuilder::build_vector_search_query(
            "semantic_embeddings",
            &[0.1, 0.2, 0.3],
            Some(&filter),
            &SearchRequestSourceSpec::None,
            12,
            &VectorSearchOptions {
                knn: KnnOptions {
                    k: 77,
                    num_candidates: 501,
                },
                enable_explain: true,
            },
        );

        assert_query_json(
            &query,
            indoc!(
                r#"
                {
                    "track_total_hits": false,
                    "knn": {
                        "field": "semantic_embeddings.embedding",
                        "query_vector": [0.1, 0.2, 0.3],
                        "k": 77,
                        "num_candidates": 501,
                        "filter": {
                            "terms": {
                                "visibility": ["public"]
                            }
                        }
                    },
                    "_source": false,
                    "size": 12,
                    "explain": true
                }
                "#
            ),
        );
    }

    #[test]
    fn test_build_most_fields_query_without_highlight_or_explain() {
        let policy = MultiMatchPolicy {
            specs: vec![
                crate::es_helpers::MultiMatchFieldSpec {
                    field_name: "title".to_string(),
                    boost: 2.0,
                },
                crate::es_helpers::MultiMatchFieldSpec {
                    field_name: "body".to_string(),
                    boost: 1.0,
                },
            ],
        };
        let filter = SearchFilterExpr::Field {
            field: "namespace",
            op: SearchFilterOp::Prefix("org.acme".to_string()),
        };

        let query = ElasticsearchQueryBuilder::build_most_fields_search_query(
            "green energy",
            FullTextSearchTermOperator::Or,
            &policy,
            Some(&filter),
            &SearchRequestSourceSpec::All,
            &[SearchSortSpec::Relevance],
            &SearchPaginationSpec {
                limit: 15,
                offset: 30,
            },
            &TextSearchOptions::default(),
        );

        assert_query_json(
            &query,
            indoc!(
                r#"
                {
                  "query": {
                    "bool": {
                      "must": {
                        "multi_match": {
                          "query": "green energy",
                          "type": "most_fields",
                          "operator": "or",
                          "fields": ["title^2", "body^1"]
                        }
                      },
                      "filter": {
                        "prefix": {
                          "namespace": "org.acme"
                        }
                      }
                    }
                  },
                  "sort": [
                    {
                      "_score": {
                        "order": "desc"
                      }
                    }
                  ],
                  "_source": true,
                  "from": 30,
                  "size": 15
                }
                "#
            ),
        );
    }
}
