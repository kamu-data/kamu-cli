// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use kamu_search::{SearchFilterExpr, field_eq_str};
use kamu_search_elasticsearch::testing::ElasticsearchTestContext;

use super::dataset_search_semantic_harness::{
    DatasetSearchSemanticHarness,
    SemanticEmbeddingsFixture,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_hybrid_search_using_description_keywords_and_attachments(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_source_field_datasets("hybrid").await;

    let by_description = harness
        .search()
        .hybrid_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_DESCRIPTION)
        .await;
    assert_eq!(
        by_description.ids().first(),
        Some(&datasets.description.to_string())
    );

    let by_keywords = harness
        .search()
        .hybrid_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_KEYWORDS)
        .await;
    assert_eq!(
        by_keywords.ids().first(),
        Some(&datasets.keywords.to_string())
    );

    let by_attachments = harness
        .search()
        .hybrid_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_ATTACHMENTS)
        .await;
    assert_eq!(
        by_attachments.ids().first(),
        Some(&datasets.attachments.to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_hybrid_search_blends_textual_and_vector_leaders_50_50(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_hybrid_blend_datasets("hybrid-blend").await;

    let text_res = harness
        .search()
        .search_dataset(SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS)
        .await;
    let vector_res = harness
        .search()
        .vector_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS)
        .await;
    let hybrid_res = harness
        .search()
        .hybrid_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS)
        .await;

    assert_eq!(
        text_res.ids().first(),
        Some(&datasets.lexical_anchor.to_string())
    );
    assert_eq!(
        vector_res.ids().first(),
        Some(&datasets.semantic_anchor.to_string())
    );

    let top_two = &hybrid_res.ids()[0..2];
    assert!(top_two.contains(&datasets.lexical_anchor.to_string()));
    assert!(top_two.contains(&datasets.semantic_anchor.to_string()));
    assert_ne!(text_res.ids().first(), vector_res.ids().first());

    assert_ne!(
        hybrid_res.ids().first(),
        Some(&datasets.unrelated.to_string()),
        "Unrelated dataset should not outrank lexical/semantic leaders"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_hybrid_search_respects_filter(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_overlap_datasets("hybrid").await;

    let filter: SearchFilterExpr = field_eq_str(
        kamu_datasets::dataset_search_schema::fields::DATASET_NAME,
        "hybrid-energy-trading",
    );

    let res = harness
        .search()
        .hybrid_search_dataset(
            SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS,
            Some(filter),
            10,
        )
        .await;

    assert_eq!(res.ids(), vec![datasets.energy_trading.to_string()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_hybrid_search_respects_limit(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_overlap_datasets("hybrid").await;

    let res = harness
        .search()
        .hybrid_search_dataset(
            SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS,
            None,
            2,
        )
        .await;

    assert_eq!(res.ids().len(), 2);

    let ids = res.ids();
    assert!(ids.contains(&datasets.climate_energy_forecast.to_string()));
    assert!(
        ids.contains(&datasets.energy_trading.to_string())
            || ids.contains(&datasets.climate_policy.to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
