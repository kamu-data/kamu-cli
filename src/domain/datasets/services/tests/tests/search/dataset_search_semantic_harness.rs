// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;
use std::sync::Arc;

use kamu_core::TenancyConfig;
use kamu_datasets::ResolvedDataset;
use kamu_search_elasticsearch::testing::ElasticsearchTestContext;
use odf::metadata::testing::MetadataFactory;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::dataset_search_harness::DatasetSearchHarness;
use crate::tests::search::es_dataset_base_harness::PrecomputedEmbeddingsConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct SourceFieldDatasets {
    pub description: odf::DatasetID,
    pub keywords: odf::DatasetID,
    pub attachments: odf::DatasetID,
}

pub(super) struct OverlapDatasets {
    pub climate_energy_forecast: odf::DatasetID,
    pub energy_trading: odf::DatasetID,
    pub climate_policy: odf::DatasetID,
    pub agronomy_weather: odf::DatasetID,
    pub sports: odf::DatasetID,
}

pub(super) struct HybridBlendDatasets {
    pub lexical_anchor: odf::DatasetID,
    pub semantic_anchor: odf::DatasetID,
    pub unrelated: odf::DatasetID,
}

pub(super) struct DatasetSearchSemanticHarness {
    inner: DatasetSearchHarness,
}

impl DatasetSearchSemanticHarness {
    pub(super) async fn new(ctx: Arc<ElasticsearchTestContext>) -> Self {
        let inner = DatasetSearchHarness::builder()
            .ctx(ctx)
            .tenancy_config(TenancyConfig::SingleTenant)
            .precomputed_embeddings_config(
                SemanticEmbeddingsFixture::precomputed_embeddings_config(),
            )
            .build()
            .await;

        Self { inner }
    }

    pub(super) fn search(&self) -> &DatasetSearchHarness {
        &self.inner
    }

    pub(super) async fn create_source_field_datasets(&self, prefix: &str) -> SourceFieldDatasets {
        let description = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "desc-dataset"),
                SemanticEmbeddingsFixture::TEXT_DATASET_DESCRIPTION,
            )
            .await;

        let keywords = self
            .create_dataset_with_keywords(
                &self.dataset_name(prefix, "keywords-dataset"),
                SemanticEmbeddingsFixture::TEXT_DATASET_KEYWORD_1,
                SemanticEmbeddingsFixture::TEXT_DATASET_KEYWORD_2,
            )
            .await;

        let attachments = self
            .create_dataset_with_attachment(
                &self.dataset_name(prefix, "attachments-dataset"),
                SemanticEmbeddingsFixture::TEXT_DATASET_ATTACHMENT,
            )
            .await;

        self.create_dataset_with_description(
            &self.dataset_name(prefix, "unrelated-dataset"),
            SemanticEmbeddingsFixture::TEXT_DATASET_UNRELATED_DESCRIPTION,
        )
        .await;

        SourceFieldDatasets {
            description,
            keywords,
            attachments,
        }
    }

    pub(super) async fn create_overlap_datasets(&self, prefix: &str) -> OverlapDatasets {
        let climate_energy_forecast = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "climate-energy-forecast"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_1_DESCRIPTION,
            )
            .await;

        let energy_trading = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "energy-trading"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_2_DESCRIPTION,
            )
            .await;

        let climate_policy = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "climate-policy"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_3_DESCRIPTION,
            )
            .await;

        let agronomy_weather = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "agronomy-weather"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_4_DESCRIPTION,
            )
            .await;

        let sports = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "sports"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_5_DESCRIPTION,
            )
            .await;

        OverlapDatasets {
            climate_energy_forecast,
            energy_trading,
            climate_policy,
            agronomy_weather,
            sports,
        }
    }

    pub(super) async fn create_hybrid_blend_datasets(&self, prefix: &str) -> HybridBlendDatasets {
        let lexical_anchor = self
            .create_dataset_with_description(
                &self.dataset_name(
                    prefix,
                    "forecast-renewable-energy-risk-climate-scenarios-anchor",
                ),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_5_DESCRIPTION,
            )
            .await;

        let semantic_anchor = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "semantic-anchor"),
                SemanticEmbeddingsFixture::TEXT_OVERLAP_1_DESCRIPTION,
            )
            .await;

        let unrelated = self
            .create_dataset_with_description(
                &self.dataset_name(prefix, "totally-unrelated"),
                SemanticEmbeddingsFixture::TEXT_DATASET_UNRELATED_DESCRIPTION,
            )
            .await;

        HybridBlendDatasets {
            lexical_anchor,
            semantic_anchor,
            unrelated,
        }
    }

    async fn create_dataset_with_description(
        &self,
        dataset_name: &str,
        description: &str,
    ) -> odf::DatasetID {
        let res = self
            .inner
            .create_root_dataset(
                self.inner.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        self.inner
            .append_dataset_metadata(
                self.inner.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info().description(description).build(),
                )],
            )
            .await;

        res.dataset_handle.id
    }

    async fn create_dataset_with_keywords(
        &self,
        dataset_name: &str,
        keyword_1: &str,
        keyword_2: &str,
    ) -> odf::DatasetID {
        let res = self
            .inner
            .create_root_dataset(
                self.inner.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        self.inner
            .append_dataset_metadata(
                self.inner.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .keyword(keyword_1)
                        .keyword(keyword_2)
                        .build(),
                )],
            )
            .await;

        res.dataset_handle.id
    }

    async fn create_dataset_with_attachment(
        &self,
        dataset_name: &str,
        attachment: &str,
    ) -> odf::DatasetID {
        let res = self
            .inner
            .create_root_dataset(
                self.inner.system_user_catalog(),
                &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name)),
            )
            .await;

        self.inner
            .append_dataset_metadata(
                self.inner.system_user_catalog(),
                ResolvedDataset::from_created(&res),
                vec![odf::MetadataEvent::SetAttachments(
                    odf::metadata::SetAttachments {
                        attachments: odf::metadata::Attachments::Embedded(
                            odf::metadata::AttachmentsEmbedded {
                                items: vec![odf::metadata::AttachmentEmbedded {
                                    path: "MANUAL.md".to_string(),
                                    content: attachment.to_string(),
                                }],
                            },
                        ),
                    },
                )],
            )
            .await;

        res.dataset_handle.id
    }

    fn dataset_name(&self, prefix: &str, suffix: &str) -> String {
        format!("{prefix}-{suffix}")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) struct SemanticEmbeddingsFixture;

impl SemanticEmbeddingsFixture {
    pub(super) const TEXT_DATASET_DESCRIPTION: &'static str =
        "Urban mobility analytics for city traffic optimization";
    pub(super) const TEXT_DATASET_KEYWORD_1: &'static str = "wind-energy";
    pub(super) const TEXT_DATASET_KEYWORD_2: &'static str = "power-grid-balancing";
    pub(super) const TEXT_DATASET_ATTACHMENT: &'static str =
        "Convolutional neural network tutorial for image recognition";
    pub(super) const TEXT_DATASET_UNRELATED_DESCRIPTION: &'static str =
        "Cooking recipes for homemade sourdough bread";

    pub(super) const QUERY_FOR_DESCRIPTION: &'static str =
        "How can I optimize traffic flow in a city?";
    pub(super) const QUERY_FOR_KEYWORDS: &'static str = "electricity generation from wind turbines";
    pub(super) const QUERY_FOR_ATTACHMENTS: &'static str = "deep learning computer vision guide";
    pub(super) const QUERY_FOR_OVERLAPPING_TOPICS: &'static str =
        "Forecast renewable energy risk under climate scenarios";

    pub(super) const TEXT_OVERLAP_1_DESCRIPTION: &'static str =
        "Climate risk forecasting for renewable energy portfolio";
    pub(super) const TEXT_OVERLAP_2_DESCRIPTION: &'static str =
        "Power market trading and renewable generation forecasts";
    pub(super) const TEXT_OVERLAP_3_DESCRIPTION: &'static str =
        "Climate policy analysis and carbon regulation impacts";
    pub(super) const TEXT_OVERLAP_4_DESCRIPTION: &'static str =
        "Agronomy weather patterns and crop yield forecasting";
    pub(super) const TEXT_OVERLAP_5_DESCRIPTION: &'static str =
        "Sports analytics for football match outcomes";

    pub(super) fn precomputed_embeddings_config() -> PrecomputedEmbeddingsConfig {
        let fixture: VectorEmbeddingsFixture =
            serde_json::from_str(VECTOR_EMBEDDINGS_FIXTURE).unwrap();

        let input_texts = Self::normalize_fixture_input_texts(VECTOR_EMBEDDINGS_INPUT);
        let expected_input_sha = Self::compute_input_sha256(&input_texts);
        assert_eq!(fixture.input_sha256, expected_input_sha);

        for (text, vector) in &fixture.vectors {
            assert_eq!(
                vector.len(),
                fixture.dimensions,
                "Embedding vector has unexpected dimensions for text: {text}"
            );
        }

        PrecomputedEmbeddingsConfig {
            dimensions: fixture.dimensions,
            vectors: fixture.vectors,
        }
    }

    fn normalize_fixture_input_texts(input: &str) -> Vec<String> {
        let mut texts = Vec::new();
        let mut seen = HashSet::new();

        for line in input.lines() {
            let text = line.trim();
            if text.is_empty() || text.starts_with('#') {
                continue;
            }

            if seen.insert(text.to_string()) {
                texts.push(text.to_string());
            }
        }

        texts
    }

    fn compute_input_sha256(texts: &[String]) -> String {
        let mut hasher = Sha256::new();

        for (idx, text) in texts.iter().enumerate() {
            if idx > 0 {
                hasher.update(b"\n");
            }
            hasher.update(text.as_bytes());
        }

        let digest = hasher.finalize();
        let mut out = String::with_capacity(digest.len() * 2);
        for byte in digest {
            write!(&mut out, "{byte:02x}").unwrap();
        }
        out
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const VECTOR_EMBEDDINGS_FIXTURE: &str = include_str!("data/vector_embeddings.json");
const VECTOR_EMBEDDINGS_INPUT: &str = include_str!("data/vector_embeddings_input.txt");

#[derive(Debug, Deserialize)]
struct VectorEmbeddingsFixture {
    dimensions: usize,
    input_sha256: String,
    vectors: HashMap<String, Vec<f32>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
