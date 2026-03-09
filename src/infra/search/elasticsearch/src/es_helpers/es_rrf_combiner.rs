// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use kamu_search::{RRFOptions, SearchEntitySchemaName, SearchHit, SearchResponse};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ElasticsearchRRFCombiner {}

impl ElasticsearchRRFCombiner {
    /// Implements Reciprocal Rank Fusion (RRF) to combine two search responses:
    ///   one from textual search and another from vector (semantic) search.
    /// The formula used is:
    ///     score(d) = `W_text` * (1 / (k + `rank_text`(d)))
    ///              + `W_vector` * (1 / (k + `rank_vector`(d)))
    ///
    /// where
    ///     k is the rank constant from `RRFOptions`,
    ///     `W_text` and `W_vector` are the weights from `RRFOptions`.
    ///
    /// The final output is sorted by the combined score.
    pub fn combine_search_responses(
        text_response: SearchResponse,
        vector_response: SearchResponse,
        rrf_options: RRFOptions,
        limit: usize,
    ) -> SearchResponse {
        let mut map: HashMap<DocKey, Acc> = HashMap::new();

        for (i, hit) in text_response.hits.into_iter().enumerate() {
            let key = DocKey {
                schema: hit.schema_name,
                id: hit.id.clone(),
            };
            let rank = i + 1;
            let entry = map.entry(key).or_default();
            entry.fused +=
                Self::rrf_inc(rrf_options.rank_constant, rank) * rrf_options.textual_weight;
            entry.text_rank = Some(rank);
            // Keep the textual hit (for highlighting/explain/source)
            entry.text_hit = Some(hit);
        }

        for (i, hit) in vector_response.hits.into_iter().enumerate() {
            let key = DocKey {
                schema: hit.schema_name,
                id: hit.id.clone(),
            };
            let rank = i + 1;
            let entry = map.entry(key).or_default();
            entry.fused +=
                Self::rrf_inc(rrf_options.rank_constant, rank) * rrf_options.vector_weight;
            entry.vector_rank = Some(rank);
            entry.vector_hit = Some(hit);
        }

        let docs_in_both = map
            .values()
            .filter(|acc| acc.text_hit.is_some() && acc.vector_hit.is_some())
            .count();

        tracing::debug!(
            total_unique_docs = map.len(),
            docs_in_both = docs_in_both,
            docs_text_only = map.len().saturating_sub(docs_in_both).saturating_sub(
                map.values()
                    .filter(|acc| acc.vector_hit.is_some() && acc.text_hit.is_none())
                    .count()
            ),
            docs_vector_only = map
                .values()
                .filter(|acc| acc.vector_hit.is_some() && acc.text_hit.is_none())
                .count(),
            "RRF merge statistics"
        );

        // Materialize and sort
        let mut fused: Vec<(DocKey, Acc)> = map.into_iter().collect();
        fused.sort_by(|a, b| {
            // Primary: fused desc
            b.1.fused
                .partial_cmp(&a.1.fused)
                .unwrap()
                // Secondary: best of ranks (lower is better)
                .then_with(|| {
                    let ar =
                        a.1.text_rank
                            .unwrap_or(usize::MAX)
                            .min(a.1.vector_rank.unwrap_or(usize::MAX));
                    let br =
                        b.1.text_rank
                            .unwrap_or(usize::MAX)
                            .min(b.1.vector_rank.unwrap_or(usize::MAX));
                    ar.cmp(&br)
                })
                // Tertiary: stable tie-breaker by schema+id
                .then_with(|| a.0.schema.cmp(b.0.schema))
                .then_with(|| a.0.id.cmp(&b.0.id))
        });

        // Select top N and choose/merge payload
        let mut hits: Vec<SearchHit> = Vec::with_capacity(limit);

        for (_key, acc) in fused.into_iter().take(limit) {
            // Choose a base hit (prefer textual for highlights, or semantic if you want)
            let out = match (acc.text_hit, acc.vector_hit) {
                (Some(textual_hit), Some(vector_hit)) => {
                    // Create a merged explanation with details from both searches
                    let explanation = Self::create_rrf_explanation(
                        acc.fused,
                        acc.text_rank,
                        textual_hit.score,
                        textual_hit.explanation.as_ref(),
                        acc.vector_rank,
                        vector_hit.score,
                        vector_hit.explanation.as_ref(),
                        &rrf_options,
                    );

                    SearchHit {
                        score: Some(acc.fused),
                        schema_name: textual_hit.schema_name,
                        id: textual_hit.id,
                        source: textual_hit.source,
                        highlights: textual_hit.highlights.or(vector_hit.highlights),
                        explanation,
                    }
                }
                (Some(textual_hit), None) => {
                    let explanation = Self::create_rrf_explanation(
                        acc.fused,
                        acc.text_rank,
                        textual_hit.score,
                        textual_hit.explanation.as_ref(),
                        None,
                        None,
                        None,
                        &rrf_options,
                    );

                    SearchHit {
                        score: Some(acc.fused),
                        schema_name: textual_hit.schema_name,
                        id: textual_hit.id,
                        source: textual_hit.source,
                        highlights: textual_hit.highlights,
                        explanation,
                    }
                }
                (None, Some(vector_hit)) => {
                    let explanation = Self::create_rrf_explanation(
                        acc.fused,
                        None,
                        None,
                        None,
                        acc.vector_rank,
                        vector_hit.score,
                        vector_hit.explanation.as_ref(),
                        &rrf_options,
                    );

                    SearchHit {
                        score: Some(acc.fused),
                        schema_name: vector_hit.schema_name,
                        id: vector_hit.id,
                        source: vector_hit.source,
                        highlights: vector_hit.highlights,
                        explanation,
                    }
                }
                (None, None) => continue,
            };

            hits.push(out);
        }

        tracing::debug!(
            fused_hits = hits.len(),
            max_score = hits.first().and_then(|h| h.score),
            min_score = hits.last().and_then(|h| h.score),
            "RRF fusion completed"
        );

        SearchResponse {
            // these were run in parallel
            took_ms: text_response.took_ms.max(vector_response.took_ms),
            timeout: text_response.timeout || vector_response.timeout,
            total_hits: None, // for hybrid, exact total is not meaningful
            hits,
        }
    }

    #[allow(clippy::cast_precision_loss)]
    fn rrf_inc(rank_constant: usize, rank_1_based: usize) -> f64 {
        // Rank values in search results are typically small (< 1000), so precision loss
        // is not a concern
        let rank = rank_constant + rank_1_based;
        let rank = rank as f64;
        1.0 / rank
    }

    fn create_rrf_explanation(
        fused_score: f64,
        text_rank: Option<usize>,
        text_score: Option<f64>,
        text_explanation: Option<&serde_json::Value>,
        vector_rank: Option<usize>,
        vector_score: Option<f64>,
        vector_explanation: Option<&serde_json::Value>,
        rrf_options: &RRFOptions,
    ) -> Option<serde_json::Value> {
        // Only create RRF explanation if at least one component has an explanation
        if text_explanation.is_none() && vector_explanation.is_none() {
            return None;
        }

        let mut explanation_obj = serde_json::Map::new();
        explanation_obj.insert(
            "description".to_string(),
            serde_json::json!("Reciprocal Rank Fusion (RRF) combined score"),
        );
        explanation_obj.insert("value".to_string(), serde_json::json!(fused_score));

        let mut details = Vec::new();

        // Add text search details
        if let Some(rank) = text_rank {
            let rrf_contribution =
                Self::rrf_inc(rrf_options.rank_constant, rank) * rrf_options.textual_weight;
            let mut text_detail = serde_json::Map::new();
            text_detail.insert(
                "description".to_string(),
                serde_json::json!("Textual search component"),
            );
            text_detail.insert("rank".to_string(), serde_json::json!(rank));
            text_detail.insert(
                "weight".to_string(),
                serde_json::json!(rrf_options.textual_weight),
            );
            text_detail.insert(
                "rrf_contribution".to_string(),
                serde_json::json!(rrf_contribution),
            );

            if let Some(score) = text_score {
                text_detail.insert("original_score".to_string(), serde_json::json!(score));
            }

            if let Some(explanation) = text_explanation {
                text_detail.insert("original_explanation".to_string(), explanation.clone());
            }

            details.push(serde_json::Value::Object(text_detail));
        }

        // Add vector search details
        if let Some(rank) = vector_rank {
            let rrf_contribution =
                Self::rrf_inc(rrf_options.rank_constant, rank) * rrf_options.vector_weight;
            let mut vector_detail = serde_json::Map::new();
            vector_detail.insert(
                "description".to_string(),
                serde_json::json!("Vector search component"),
            );
            vector_detail.insert("rank".to_string(), serde_json::json!(rank));
            vector_detail.insert(
                "weight".to_string(),
                serde_json::json!(rrf_options.vector_weight),
            );
            vector_detail.insert(
                "rrf_contribution".to_string(),
                serde_json::json!(rrf_contribution),
            );

            if let Some(score) = vector_score {
                vector_detail.insert("original_score".to_string(), serde_json::json!(score));
            }

            if let Some(explanation) = vector_explanation {
                vector_detail.insert("original_explanation".to_string(), explanation.clone());
            }

            details.push(serde_json::Value::Object(vector_detail));
        }

        explanation_obj.insert("details".to_string(), serde_json::Value::Array(details));

        Some(serde_json::Value::Object(explanation_obj))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct DocKey {
    schema: SearchEntitySchemaName,
    id: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct Acc {
    fused: f64,
    text_rank: Option<usize>,
    vector_rank: Option<usize>,
    text_hit: Option<SearchHit>,
    vector_hit: Option<SearchHit>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use kamu_search::{RRFOptions, SearchHighlight};
    use serde_json::json;

    use super::*;

    fn make_hit(
        schema_name: SearchEntitySchemaName,
        id: &str,
        score: Option<f64>,
        source: serde_json::Value,
        highlights: Option<Vec<SearchHighlight>>,
        explanation: Option<serde_json::Value>,
    ) -> SearchHit {
        SearchHit {
            id: id.to_string(),
            schema_name,
            score,
            highlights,
            source,
            explanation,
        }
    }

    fn make_response(took_ms: u64, timeout: bool, hits: Vec<SearchHit>) -> SearchResponse {
        SearchResponse {
            took_ms,
            timeout,
            total_hits: Some(999),
            hits,
        }
    }

    fn response_to_json(response: &SearchResponse) -> serde_json::Value {
        json!({
            "took_ms": response.took_ms,
            "timeout": response.timeout,
            "total_hits": response.total_hits,
            "hits": response
                .hits
                .iter()
                .map(|hit| {
                    json!({
                        "id": hit.id,
                        "schema_name": hit.schema_name,
                        "score": hit.score,
                        "highlights": hit.highlights.as_ref().map(|all| {
                            all.iter()
                                .map(|h| {
                                    json!({
                                        "field": h.field,
                                        "best_fragment": h.best_fragment,
                                    })
                                })
                                .collect::<Vec<_>>()
                        }),
                        "source": hit.source,
                        "explanation": hit.explanation,
                    })
                })
                .collect::<Vec<_>>(),
        })
    }

    #[test]
    fn test_combine_search_responses_merges_both_sources_with_full_rrf_explanation() {
        let rrf_options = RRFOptions {
            rank_window_size: 100,
            rank_constant: 60,
            textual_weight: 1.0,
            vector_weight: 2.0,
        };

        let text_response = make_response(
            11,
            false,
            vec![make_hit(
                "dataset",
                "doc-1",
                Some(10.0),
                json!({"origin": "text"}),
                Some(vec![SearchHighlight {
                    field: "title".to_string(),
                    best_fragment: "text fragment".to_string(),
                }]),
                Some(json!({"text": "exp"})),
            )],
        );

        let vector_response = make_response(
            17,
            false,
            vec![make_hit(
                "dataset",
                "doc-1",
                Some(0.9),
                json!({"origin": "vector"}),
                Some(vec![SearchHighlight {
                    field: "description".to_string(),
                    best_fragment: "vector fragment".to_string(),
                }]),
                Some(json!({"vector": "exp"})),
            )],
        );

        let response = ElasticsearchRRFCombiner::combine_search_responses(
            text_response,
            vector_response,
            rrf_options,
            10,
        );

        let expected_score = ElasticsearchRRFCombiner::rrf_inc(60, 1) * 1.0
            + ElasticsearchRRFCombiner::rrf_inc(60, 1) * 2.0;

        assert_eq!(
            response_to_json(&response),
            json!({
                "took_ms": 17,
                "timeout": false,
                "total_hits": null,
                "hits": [
                    {
                        "id": "doc-1",
                        "schema_name": "dataset",
                        "score": expected_score,
                        "highlights": [
                            {
                                "field": "title",
                                "best_fragment": "text fragment",
                            }
                        ],
                        "source": {"origin": "text"},
                        "explanation": {
                            "description": "Reciprocal Rank Fusion (RRF) combined score",
                            "value": expected_score,
                            "details": [
                                {
                                    "description": "Textual search component",
                                    "rank": 1,
                                    "weight": 1.0,
                                    "rrf_contribution": ElasticsearchRRFCombiner::rrf_inc(60, 1) * 1.0,
                                    "original_score": 10.0,
                                    "original_explanation": {"text": "exp"},
                                },
                                {
                                    "description": "Vector search component",
                                    "rank": 1,
                                    "weight": 2.0,
                                    "rrf_contribution": ElasticsearchRRFCombiner::rrf_inc(60, 1) * 2.0,
                                    "original_score": 0.9,
                                    "original_explanation": {"vector": "exp"},
                                }
                            ]
                        },
                    }
                ]
            })
        );
    }

    #[test]
    fn test_combine_search_responses_uses_vector_highlights_if_textual_missing() {
        let rrf_options = RRFOptions {
            rank_window_size: 100,
            rank_constant: 60,
            textual_weight: 1.0,
            vector_weight: 1.0,
        };

        let text_response = make_response(
            7,
            false,
            vec![make_hit(
                "dataset",
                "doc-1",
                Some(6.0),
                json!({"origin": "text"}),
                None,
                Some(json!({"text": "exp"})),
            )],
        );

        let vector_response = make_response(
            5,
            false,
            vec![make_hit(
                "dataset",
                "doc-1",
                Some(0.5),
                json!({"origin": "vector"}),
                Some(vec![SearchHighlight {
                    field: "description".to_string(),
                    best_fragment: "vector fragment".to_string(),
                }]),
                Some(json!({"vector": "exp"})),
            )],
        );

        let response = ElasticsearchRRFCombiner::combine_search_responses(
            text_response,
            vector_response,
            rrf_options,
            10,
        );

        let expected_score = ElasticsearchRRFCombiner::rrf_inc(60, 1) * 1.0
            + ElasticsearchRRFCombiner::rrf_inc(60, 1) * 1.0;

        assert_eq!(
            response_to_json(&response),
            json!({
                "took_ms": 7,
                "timeout": false,
                "total_hits": null,
                "hits": [
                    {
                        "id": "doc-1",
                        "schema_name": "dataset",
                        "score": expected_score,
                        "highlights": [
                            {
                                "field": "description",
                                "best_fragment": "vector fragment",
                            }
                        ],
                        "source": {"origin": "text"},
                        "explanation": {
                            "description": "Reciprocal Rank Fusion (RRF) combined score",
                            "value": expected_score,
                            "details": [
                                {
                                    "description": "Textual search component",
                                    "rank": 1,
                                    "weight": 1.0,
                                    "rrf_contribution": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                                    "original_score": 6.0,
                                    "original_explanation": {"text": "exp"},
                                },
                                {
                                    "description": "Vector search component",
                                    "rank": 1,
                                    "weight": 1.0,
                                    "rrf_contribution": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                                    "original_score": 0.5,
                                    "original_explanation": {"vector": "exp"},
                                }
                            ]
                        },
                    }
                ]
            })
        );
    }

    #[test]
    fn test_combine_search_responses_handles_text_only_and_vector_only_without_explanations() {
        let rrf_options = RRFOptions {
            rank_window_size: 100,
            rank_constant: 60,
            textual_weight: 1.0,
            vector_weight: 1.0,
        };

        let text_response = make_response(
            3,
            true,
            vec![make_hit(
                "dataset",
                "text-only",
                Some(4.0),
                json!({"kind": "text"}),
                Some(vec![SearchHighlight {
                    field: "title".to_string(),
                    best_fragment: "text only".to_string(),
                }]),
                None,
            )],
        );

        let vector_response = make_response(
            9,
            false,
            vec![make_hit(
                "dataset",
                "vector-only",
                Some(0.8),
                json!({"kind": "vector"}),
                Some(vec![SearchHighlight {
                    field: "description".to_string(),
                    best_fragment: "vector only".to_string(),
                }]),
                None,
            )],
        );

        let response = ElasticsearchRRFCombiner::combine_search_responses(
            text_response,
            vector_response,
            rrf_options,
            10,
        );

        assert_eq!(
            response_to_json(&response),
            json!({
                "took_ms": 9,
                "timeout": true,
                "total_hits": null,
                "hits": [
                    {
                        "id": "text-only",
                        "schema_name": "dataset",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": [
                            {
                                "field": "title",
                                "best_fragment": "text only",
                            }
                        ],
                        "source": {"kind": "text"},
                        "explanation": null,
                    },
                    {
                        "id": "vector-only",
                        "schema_name": "dataset",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": [
                            {
                                "field": "description",
                                "best_fragment": "vector only",
                            }
                        ],
                        "source": {"kind": "vector"},
                        "explanation": null,
                    }
                ]
            })
        );
    }

    #[test]
    fn test_combine_search_responses_applies_stable_tie_breaker_by_schema_and_id() {
        let rrf_options = RRFOptions {
            rank_window_size: 100,
            rank_constant: 60,
            textual_weight: 1.0,
            vector_weight: 1.0,
        };

        let text_response = make_response(
            1,
            false,
            vec![make_hit(
                "z-schema",
                "z-id",
                Some(1.0),
                json!({"from": "text"}),
                None,
                None,
            )],
        );

        let vector_response = make_response(
            1,
            false,
            vec![make_hit(
                "a-schema",
                "a-id",
                Some(0.1),
                json!({"from": "vector"}),
                None,
                None,
            )],
        );

        let response = ElasticsearchRRFCombiner::combine_search_responses(
            text_response,
            vector_response,
            rrf_options,
            10,
        );

        assert_eq!(
            response_to_json(&response),
            json!({
                "took_ms": 1,
                "timeout": false,
                "total_hits": null,
                "hits": [
                    {
                        "id": "a-id",
                        "schema_name": "a-schema",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": null,
                        "source": {"from": "vector"},
                        "explanation": null,
                    },
                    {
                        "id": "z-id",
                        "schema_name": "z-schema",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": null,
                        "source": {"from": "text"},
                        "explanation": null,
                    }
                ]
            })
        );
    }

    #[test]
    fn test_combine_search_responses_respects_limit() {
        let rrf_options = RRFOptions {
            rank_window_size: 100,
            rank_constant: 60,
            textual_weight: 1.0,
            vector_weight: 1.0,
        };

        let text_response = make_response(
            5,
            false,
            vec![
                make_hit("dataset", "doc-1", Some(9.0), json!({"idx": 1}), None, None),
                make_hit("dataset", "doc-2", Some(8.0), json!({"idx": 2}), None, None),
            ],
        );

        let vector_response = make_response(
            6,
            false,
            vec![make_hit(
                "dataset",
                "doc-3",
                Some(0.4),
                json!({"idx": 3}),
                None,
                None,
            )],
        );

        let response = ElasticsearchRRFCombiner::combine_search_responses(
            text_response,
            vector_response,
            rrf_options,
            2,
        );

        assert_eq!(
            response_to_json(&response),
            json!({
                "took_ms": 6,
                "timeout": false,
                "total_hits": null,
                "hits": [
                    {
                        "id": "doc-1",
                        "schema_name": "dataset",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": null,
                        "source": {"idx": 1},
                        "explanation": null,
                    },
                    {
                        "id": "doc-3",
                        "schema_name": "dataset",
                        "score": ElasticsearchRRFCombiner::rrf_inc(60, 1),
                        "highlights": null,
                        "source": {"idx": 3},
                        "explanation": null,
                    }
                ]
            })
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
