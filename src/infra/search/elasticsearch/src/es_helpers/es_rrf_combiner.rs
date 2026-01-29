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
