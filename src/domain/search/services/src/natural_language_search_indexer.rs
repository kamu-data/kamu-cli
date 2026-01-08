// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use futures::TryStreamExt;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use itertools::Itertools;
use kamu_datasets::{DatasetRegistry, JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER, ResolvedDataset};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct NaturalLanguageSearchIndexerConfig {
    // Whether to clear and re-index on start or work with existing vectors is any exist
    pub clear_on_start: bool,

    /// Whether to skip indexing datasets that have no readme or description
    pub skip_datasets_with_no_description: bool,

    /// Whether to skip indexing datasets that have no data
    pub skip_datasets_with_no_data: bool,

    /// Whether to include the original text as payload of the vectors when
    /// storing them. It is not needed for normal service operations but can
    /// help debug issues.
    pub payload_include_content: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.NaturalLanguageSearchIndexer",
    depends_on: &[JOB_KAMU_DATASETS_DATASET_BLOCK_INDEXER],
    requires_transaction: false,
})]
pub struct NaturalLanguageSearchIndexer {
    catalog: dill::Catalog,
    config: Arc<NaturalLanguageSearchIndexerConfig>,
    embeddings_chunker: Arc<dyn EmbeddingsChunker>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
    vector_repo: Arc<dyn VectorRepository>,
}

impl NaturalLanguageSearchIndexer {
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn index_datasets(&self) -> Result<(), InternalError> {
        tracing::info!("Fetching metadata of all datasets. This make take a while!");

        let mut chunks = Vec::new();
        let mut payloads = Vec::new();
        let mut datasets = dataset_registry.all_dataset_handles();

        while let Some(hdl) = datasets.try_next().await? {
            let dataset = dataset_registry.get_dataset_by_handle(&hdl).await;

            let Some(document) = self.dataset_metadata_as_document(dataset).await? else {
                continue;
            };

            for chunk in self.embeddings_chunker.chunk(document).await? {
                let payload = if !self.config.payload_include_content {
                    serde_json::json!({
                        "dataset_id": hdl.id.to_string(),
                    })
                } else {
                    serde_json::json!({
                        "dataset_id": hdl.id.to_string(),
                        "dataset_alias": hdl.alias.to_string(),
                        "content": chunk,
                    })
                };

                payloads.push(payload);
                chunks.push(chunk);
            }
        }

        tracing::info!(?chunks, "Encoding chunks to embeddings");

        // TODO: Split into batches?
        let vectors = self.embeddings_encoder.encode(chunks).await?;

        let points = vectors
            .into_iter()
            .zip_eq(payloads)
            .map(|(vector, payload)| NewPoint { vector, payload })
            .collect();

        self.vector_repo.insert(points).await.int_err()?;

        Ok(())
    }

    async fn dataset_metadata_as_document(
        &self,
        dataset: ResolvedDataset,
    ) -> Result<Option<Vec<String>>, InternalError> {
        let mut attachments_visitor = odf::dataset::SearchSetAttachmentsVisitor::new();
        let mut info_visitor = odf::dataset::SearchSetInfoVisitor::new();
        let mut schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();
        let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

        let mut visitors: [&mut dyn odf::dataset::MetadataChainVisitor<Error = odf::dataset::Infallible>;
            4] = [
            &mut attachments_visitor,
            &mut info_visitor,
            &mut schema_visitor,
            &mut seed_visitor,
        ];

        use odf::dataset::MetadataChainExt as _;
        dataset
            .as_metadata_chain()
            .accept(&mut visitors)
            .await
            .int_err()?;

        let attachments = attachments_visitor.into_event();
        let embedded_attachments = attachments
            .map(|a| match a.attachments {
                odf::metadata::Attachments::Embedded(a) => a.items,
            })
            .unwrap_or_default();

        let info = info_visitor.into_event().unwrap_or_default();
        let schema = schema_visitor.into_event().map(|e| e.upgrade().schema);

        let mut chunks = Vec::new();

        if self.config.skip_datasets_with_no_data && schema.is_none() {
            return Ok(None);
        }
        if self.config.skip_datasets_with_no_description
            && info.description.is_none()
            && embedded_attachments.is_empty()
        {
            return Ok(None);
        }

        // Info
        chunks.push(format!(
            "{}\n{}\n{}",
            dataset.get_alias().dataset_name.replace(['.', '-'], " "),
            info.description.unwrap_or_default(),
            info.keywords.unwrap_or_default().join(" "),
        ));

        // Schema
        chunks.push(
            schema
                .map(|s| s.fields.iter().map(|f| f.name.clone()).collect::<Vec<_>>())
                .unwrap_or_default()
                .join(" "),
        );

        // Attachments
        chunks.extend(embedded_attachments.into_iter().map(|a| a.content));

        let chunks = chunks
            .into_iter()
            .map(|c| c.trim().to_string())
            .filter(|c| !c.is_empty())
            .collect();

        Ok(Some(chunks))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl InitOnStartup for NaturalLanguageSearchIndexer {
    #[tracing::instrument(level = "info", name = NaturalLanguageSearchIndexer_run_initialization, skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        if self.config.clear_on_start {
            self.vector_repo.clear().await?;
        } else if self.vector_repo.num_points().await? != 0 {
            // Skip init if collection already exists
            return Ok(());
        }

        self.index_datasets().await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
