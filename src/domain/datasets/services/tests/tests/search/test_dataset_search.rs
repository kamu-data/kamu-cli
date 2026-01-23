// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use bon::bon;
use kamu_accounts::PredefinedAccountsConfig;
use kamu_core::TenancyConfig;
use kamu_datasets::{ResolvedDataset, dataset_search_schema};
use kamu_search::*;
use kamu_search_elasticsearch::testing::{ElasticsearchTestContext, SearchTestResponse};
use odf::metadata::testing::MetadataFactory;

use super::es_dataset_base_harness::ElasticsearchDatasetBaseHarness;
use crate::tests::search::es_dataset_base_harness::PredefinedDatasetsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_no_results_in_empty_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let res = harness.search_dataset("test").await;
    assert_eq!(res.0.total_hits, Some(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_full_name_match(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    for dataset_name in &["alpha", "beta", "gamma"] {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("alpha").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["alpha"].to_string()]);

    let res = harness.search_dataset("beta").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]);

    let res = harness.search_dataset("gamma").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["gamma"].to_string()]);

    let res = harness.search_dataset("delta").await;
    assert_eq!(res.total_hits(), Some(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_datasets_by_full_name_different_case(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    for dataset_name in &["alpha", "beta"] {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("ALPHA").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["alpha"].to_string()]);

    let res = harness.search_dataset("BeTa").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_name_prefix(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    for dataset_name in &["alpha", "alphacentauri", "beta"] {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("alph").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["alpha"].to_string(),
            dataset_ids_by_name["alphacentauri"].to_string(),
        ]
    );

    let res = harness.search_dataset("bet").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]);

    let res = harness.search_dataset("Alp").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["alpha"].to_string(),
            dataset_ids_by_name["alphacentauri"].to_string(),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_name_substring(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    for dataset_name in &["alpha", "alphabet", "beta", "zetatron"] {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("lph").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // Both "alpha" and "alphabet" match via inner ngram
            // Tie-breaker by title field
            dataset_ids_by_name["alpha"].to_string(),
            dataset_ids_by_name["alphabet"].to_string(),
        ]
    );

    let res = harness.search_dataset("bet").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // "beta" scores higher, as it matches via edge-ngram.
            // "alphabet" matches only via inner ngram, which has lower boost.
            dataset_ids_by_name["beta"].to_string(),
            dataset_ids_by_name["alphabet"].to_string(),
        ]
    );

    let res = harness.search_dataset("TRO").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["zetatron"].to_string()]);

    let res = harness.search_dataset("eta").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // Both "beta" and "zetatron" match via inner ngram
            // Tie-breaker by title field
            dataset_ids_by_name["beta"].to_string(),
            dataset_ids_by_name["zetatron"].to_string(),
        ]
    );

    // Minimum 3 characters for substring search
    let res_2 = harness.search_dataset("et").await;
    assert_eq!(res_2.total_hits(), Some(0));

    // Maximum 6 characters for substring search
    let res_6 = harness.search_dataset("lphabe").await;
    assert_eq!(res_6.total_hits(), Some(1));
    assert_eq!(
        res_6.ids(),
        vec![dataset_ids_by_name["alphabet"].to_string()]
    );

    let res_7 = harness.search_dataset("lphabet").await;
    assert_eq!(res_7.total_hits(), Some(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_name_part_or_part_prefix(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    for dataset_name in &["alpha-beta", "beta-gamma", "gamma-delta"] {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("alpha").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(
        res.ids(),
        vec![dataset_ids_by_name["alpha-beta"].to_string()]
    );

    let res = harness.search_dataset("beta").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["alpha-beta"].to_string(),
            dataset_ids_by_name["beta-gamma"].to_string(),
        ]
    );

    let res = harness.search_dataset("gamma").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["beta-gamma"].to_string(),
            dataset_ids_by_name["gamma-delta"].to_string(),
        ]
    );

    let res = harness.search_dataset("delta").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(
        res.ids(),
        vec![dataset_ids_by_name["gamma-delta"].to_string()]
    );

    let res = harness.search_dataset("GAM").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["beta-gamma"].to_string(),
            dataset_ids_by_name["gamma-delta"].to_string(),
        ]
    );

    let res = harness.search_dataset("ETA").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["alpha-beta"].to_string(),
            dataset_ids_by_name["beta-gamma"].to_string(),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_description_word(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    let datasets = vec![
        ("alpha", "The first dataset in the collection"),
        ("beta", "Better late than never, says candidate's résumé"),
        ("gamma", "Speeding kills, but gamma rays are faster"),
    ];

    for (dataset_name, description) in datasets {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;
        harness
            .append_dataset_metadata(
                harness.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info().description(description).build(),
                )],
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("first").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["alpha"].to_string()]);

    let res = harness.search_dataset("better").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]);

    let res = harness.search_dataset("speeding").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["gamma"].to_string()]);

    // Ascii folding
    let res = harness.search_dataset("resume").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]); // résumé

    // Possessive stemming
    let res = harness.search_dataset("candidate").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["beta"].to_string()]); // candidate's

    // Stemming activation
    let res = harness.search_dataset("collections").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["alpha"].to_string()]); // we have "collection"

    let res = harness.search_dataset("kill").await;
    assert_eq!(res.total_hits(), Some(1)); // we have "kills"
    assert_eq!(res.ids(), vec![dataset_ids_by_name["gamma"].to_string()]);

    // Should not match stop words
    let res = harness.search_dataset("the").await;
    assert_eq!(res.total_hits(), Some(0));

    let res = harness.search_dataset("than").await;
    assert_eq!(res.total_hits(), Some(0));

    // No superlatives/comparatives
    let res = harness.search_dataset("fastest").await;
    assert_eq!(res.total_hits(), Some(0));

    let res = harness.search_dataset("fast").await;
    assert_eq!(res.total_hits(), Some(0));

    let res = harness.search_dataset("faster").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["gamma"].to_string()]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_attachment_content(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    let datasets = vec![
        (
            "delta",
            "This document however contains essential financial information",
        ),
        (
            "epsilon",
            "User's guide explains naïve Bayes algorithm thoroughly",
        ),
        (
            "zeta",
            "Running tests successfully requires proper configuration",
        ),
    ];

    for (dataset_name, attachment_content) in datasets {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;
        harness
            .append_dataset_metadata(
                harness.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetAttachments(
                    odf::metadata::SetAttachments {
                        attachments: odf::metadata::Attachments::Embedded(
                            odf::metadata::AttachmentsEmbedded {
                                items: vec![odf::metadata::AttachmentEmbedded {
                                    path: "README.md".to_string(),
                                    content: attachment_content.to_string(),
                                }],
                            },
                        ),
                    },
                )],
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    let res = harness.search_dataset("essential").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["delta"].to_string()]);

    let res = harness.search_dataset("guide").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["epsilon"].to_string()]);

    let res = harness.search_dataset("running").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["zeta"].to_string()]);

    // Ascii folding
    let res = harness.search_dataset("naive").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["epsilon"].to_string()]); // naïve

    // Possessive stemming
    let res = harness.search_dataset("user").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["epsilon"].to_string()]); // User's

    // Stemming activation
    let res = harness.search_dataset("documents").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["delta"].to_string()]); // we have "document"

    let res = harness.search_dataset("require").await;
    assert_eq!(res.total_hits(), Some(1)); // we have "requires"
    assert_eq!(res.ids(), vec![dataset_ids_by_name["zeta"].to_string()]);

    // Should not match stop words
    let res = harness.search_dataset("this").await;
    assert_eq!(res.total_hits(), Some(0));

    let res = harness.search_dataset("however").await;
    assert_eq!(res.total_hits(), Some(0));

    // No superlatives/comparatives
    let res = harness.search_dataset("successfully").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["zeta"].to_string()]);

    let res = harness.search_dataset("successful").await;
    assert_eq!(res.total_hits(), Some(0));

    let res = harness.search_dataset("success").await;
    assert_eq!(res.total_hits(), Some(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_schema_field_name(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    let mut dataset_ids_by_name = HashMap::new();
    let datasets = vec![
        ("theta", vec!["customer_id", "order_amount", "timestamp"]),
        ("iota", vec!["user_name", "café_location", "rating"]),
        ("kappa", vec!["product_sku", "quantity", "customer_email"]),
    ];

    for (dataset_name, field_names) in datasets {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        let custom_fields: Vec<Field> = field_names
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, true))
            .collect();

        let mut fields = vec![
            Field::new("offset", DataType::Int64, false),
            Field::new("op", DataType::Int32, false),
            Field::new(
                "system_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                false,
            ),
            Field::new(
                "event_time",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
        ];
        fields.extend(custom_fields);

        harness
            .append_dataset_metadata(
                harness.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetDataSchema(
                    MetadataFactory::set_data_schema()
                        .schema_from_arrow(&Schema::new(fields))
                        .build(),
                )],
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    // Full match
    let res = harness.search_dataset("customer_id").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // Score is not the same:
            //   customer_id is a migh higher score than customer_email
            // That is why tie-breaker by title field does not apply
            dataset_ids_by_name["theta"].to_string(),
            dataset_ids_by_name["kappa"].to_string(),
        ]
    );

    let res = harness.search_dataset("user_name").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["iota"].to_string()]);

    let res = harness.search_dataset("product_sku").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["kappa"].to_string()]);

    // Prefix match
    let res = harness.search_dataset("customer").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // Score is the same: customer_id vs customer_email
            // Tie-breaker by title field
            dataset_ids_by_name["kappa"].to_string(),
            dataset_ids_by_name["theta"].to_string(),
        ]
    );

    let res = harness.search_dataset("cust").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            // Score is the same: customer_id vs customer_email
            // Tie-breaker by title field
            dataset_ids_by_name["kappa"].to_string(),
            dataset_ids_by_name["theta"].to_string(),
        ]
    );

    // ASCII folding
    let res = harness.search_dataset("cafe_location").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["iota"].to_string()]); // café

    // Case insensitive
    let res = harness.search_dataset("PRODUCT_SKU").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["kappa"].to_string()]);

    let res = harness.search_dataset("User_Name").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["iota"].to_string()]);

    // No partial match within field name
    let res = harness.search_dataset("order").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["theta"].to_string()]);

    let res = harness.search_dataset("uct").await;
    assert_eq!(res.total_hits(), Some(0)); // should not match "product_sku"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_find_dataset_by_keyword(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetSearchHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let mut dataset_ids_by_name = HashMap::new();
    let datasets = vec![
        ("lambda", vec!["finance", "quarterly", "revenue"]),
        (
            "mu",
            vec!["machine-learning", "naïve-bayes", "classification"],
        ),
        ("nu", vec!["sales", "quarterly", "forecast"]),
    ];

    for (dataset_name, keywords) in datasets {
        let res = harness
            .create_root_dataset(
                harness.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        let mut set_info_builder = MetadataFactory::set_info();
        for keyword in keywords {
            set_info_builder = set_info_builder.keyword(keyword);
        }

        harness
            .append_dataset_metadata(
                harness.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetInfo(set_info_builder.build())],
            )
            .await;
        dataset_ids_by_name.insert((*dataset_name).to_string(), res.dataset_handle.id);
    }

    // Exact match
    let res = harness.search_dataset("finance").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["lambda"].to_string()]);

    let res = harness.search_dataset("machine-learning").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["mu"].to_string()]);

    let res = harness.search_dataset("sales").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["nu"].to_string()]);

    // Prefix match
    let res = harness.search_dataset("quart").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["lambda"].to_string(),
            dataset_ids_by_name["nu"].to_string(),
        ]
    );

    let res = harness.search_dataset("machine").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["mu"].to_string()]);

    // ASCII folding
    let res = harness.search_dataset("naive-bayes").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["mu"].to_string()]); // naïve

    // Case insensitive
    let res = harness.search_dataset("FINANCE").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["lambda"].to_string()]);

    let res = harness.search_dataset("Classification").await;
    assert_eq!(res.total_hits(), Some(1));
    assert_eq!(res.ids(), vec![dataset_ids_by_name["mu"].to_string()]);

    // Prefix with case insensitivity
    let res = harness.search_dataset("QUART").await;
    assert_eq!(res.total_hits(), Some(2));
    assert_eq!(
        res.ids(),
        vec![
            dataset_ids_by_name["lambda"].to_string(),
            dataset_ids_by_name["nu"].to_string(),
        ]
    );

    // No partial match within keyword
    let res = harness.search_dataset("cast").await;
    assert_eq!(res.total_hits(), Some(0)); // should not match "forecast"

    let res = harness.search_dataset("earn").await;
    assert_eq!(res.total_hits(), Some(0)); // should not match "machine-learning"
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ElasticsearchDatasetBaseHarness, es_dataset_base_use_case_harness)]
struct DatasetSearchHarness {
    es_dataset_base_use_case_harness: ElasticsearchDatasetBaseHarness,
}

#[bon]
impl DatasetSearchHarness {
    #[builder]
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        tenancy_config: TenancyConfig,
        maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
        maybe_predefined_datasets_config: Option<PredefinedDatasetsConfig>,
    ) -> Self {
        let es_dataset_base_use_case_harness = ElasticsearchDatasetBaseHarness::builder()
            .ctx(ctx)
            .tenancy_config(tenancy_config)
            .maybe_predefined_accounts_config(maybe_predefined_accounts_config)
            .maybe_predefined_datasets_config(maybe_predefined_datasets_config)
            .build()
            .await;

        Self {
            es_dataset_base_use_case_harness,
        }
    }

    async fn search_dataset(&self, query: &str) -> SearchTestResponse {
        self.synchronize().await;

        let seach_response = self
            .search_repo()
            .text_search(TextSearchRequest {
                intent: TextSearchIntent::make_full_text(query),
                entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                source: SearchRequestSourceSpec::None,
                filter: None,
                page: SearchPaginationSpec {
                    limit: 100,
                    offset: 0,
                },
                options: TextSearchOptions::default(),
            })
            .await
            .unwrap();

        SearchTestResponse(seach_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
