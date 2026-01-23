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
        #[derive(Debug, Clone, Hash, PartialEq, Eq)]
        struct DocKey {
            schema: SearchEntitySchemaName,
            id: String,
        }

        #[derive(Debug, Default)]
        struct Acc {
            fused: f64,
            text_rank: Option<usize>,
            vector_rank: Option<usize>,
            text_hit: Option<SearchHit>,
            vector_hit: Option<SearchHit>,
        }

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
                (Some(textual_hit), Some(vector_hit)) => SearchHit {
                    score: Some(acc.fused),
                    schema_name: textual_hit.schema_name,
                    id: textual_hit.id,
                    source: textual_hit.source,
                    highlights: textual_hit.highlights.or(vector_hit.highlights),
                    explanation: textual_hit.explanation.or(vector_hit.explanation),
                },
                (Some(textual_hit), None) => SearchHit {
                    score: Some(acc.fused),
                    ..textual_hit
                },
                (None, Some(vector_hit)) => SearchHit {
                    score: Some(acc.fused),
                    ..vector_hit
                },
                (None, None) => continue,
            };

            hits.push(out);
        }

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
