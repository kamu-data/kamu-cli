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
async fn test_find_dataset_by_vector_search_using_description_keywords_and_attachments(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_source_field_datasets("vector").await;

    let by_description = harness
        .search()
        .vector_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_DESCRIPTION)
        .await;
    assert_eq!(
        by_description.ids().first(),
        Some(&datasets.description.to_string())
    );

    let by_keywords = harness
        .search()
        .vector_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_KEYWORDS)
        .await;
    assert_eq!(
        by_keywords.ids().first(),
        Some(&datasets.keywords.to_string())
    );

    let by_attachments = harness
        .search()
        .vector_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_ATTACHMENTS)
        .await;
    assert_eq!(
        by_attachments.ids().first(),
        Some(&datasets.attachments.to_string())
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_vector_search_ranks_overlapping_topics_with_meaningful_order(
    ctx: Arc<ElasticsearchTestContext>,
) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_overlap_datasets("vector").await;

    let res = harness
        .search()
        .vector_search_dataset_by_prompt(SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS)
        .await;

    assert_eq!(res.ids().len(), 5);

    let ids = res.ids();
    assert_eq!(ids[0], datasets.climate_energy_forecast.to_string());
    assert_eq!(ids[1], datasets.energy_trading.to_string());

    let middle_pair = [ids[2].clone(), ids[3].clone()];
    assert!(middle_pair.contains(&datasets.climate_policy.to_string()));
    assert!(middle_pair.contains(&datasets.agronomy_weather.to_string()));

    assert_eq!(ids[4], datasets.sports.to_string());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_vector_search_respects_filter(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_overlap_datasets("vector").await;

    let filter: SearchFilterExpr = field_eq_str(
        kamu_datasets::dataset_search_schema::fields::DATASET_NAME,
        "vector-energy-trading",
    );

    let res = harness
        .search()
        .vector_search_dataset(
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
async fn test_vector_search_respects_limit(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchSemanticHarness::new(ctx).await;
    let datasets = harness.create_overlap_datasets("vector").await;

    let res = harness
        .search()
        .vector_search_dataset(
            SemanticEmbeddingsFixture::QUERY_FOR_OVERLAPPING_TOPICS,
            None,
            2,
        )
        .await;

    assert_eq!(res.ids().len(), 2);

    let ids = res.ids();
    assert!(ids.contains(&datasets.climate_energy_forecast.to_string()));
    assert!(ids.contains(&datasets.energy_trading.to_string()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
