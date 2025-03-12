// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use futures::TryStreamExt;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::*;
use itertools::Itertools;
use kamu_core::{DatasetRegistry, ResolvedDataset};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.search.SearchServiceLocalIndexer",
    depends_on: &[],
    requires_transaction: true,
})]
pub struct SearchServiceLocalIndexer {
    dataset_registry: Arc<dyn DatasetRegistry>,
    embeddings_chunker: Arc<dyn EmbeddingsChunker>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
    vector_repo: Arc<dyn VectorRepository>,
}

impl SearchServiceLocalIndexer {
    async fn dataset_to_description(
        &self,
        dataset: ResolvedDataset,
    ) -> Result<Vec<String>, InternalError> {
        let mut attachments_visitor = odf::dataset::SearchSetAttachmentsVisitor::new();
        let mut info_visitor = odf::dataset::SearchSetInfoVisitor::new();
        let mut schema_visitor = odf::dataset::SearchSetDataSchemaVisitor::new();
        let mut seed_visitor = odf::dataset::SearchSeedVisitor::new();

        let mut visitors: [&mut dyn odf::dataset::MetadataChainVisitor<
            Error = odf::dataset::Infallible,
        >; 4] = [
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
        let info = info_visitor.into_event();
        let schema = schema_visitor
            .into_event()
            .map(|schema| schema.schema_as_arrow())
            .transpose()
            .int_err()?;

        let mut chunks = Vec::new();

        // Info
        chunks.push(format!(
            "{} {}",
            dataset.get_alias().dataset_name.replace(['.', '-'], " "),
            info.map(|i| format!(
                "{} {}",
                i.description.unwrap_or_default(),
                i.keywords.unwrap_or_default().join(" ")
            ))
            .unwrap_or_default()
        ));

        // Schema
        chunks.push(
            schema
                .map(|s| {
                    s.fields
                        .iter()
                        .map(|f| f.name().clone())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
                .join(" "),
        );

        // Attachments
        let embedded_attachments = attachments
            .map(|a| match a.attachments {
                odf::metadata::Attachments::Embedded(a) => a.items,
            })
            .unwrap_or_default();

        for attachment in embedded_attachments {
            chunks.push(attachment.content);
        }

        chunks.iter_mut().for_each(|i| (*i) = i.trim().to_string());
        chunks.retain(|i| !i.is_empty());

        Ok(chunks)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl InitOnStartup for SearchServiceLocalIndexer {
    #[tracing::instrument(level = "info", skip_all)]
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Skip init if collection already exists
        if self.vector_repo.num_points().await? != 0 {
            return Ok(());
        }

        let mut chunks = Vec::new();
        let mut payloads = Vec::new();
        let mut datasets = self.dataset_registry.all_dataset_handles();

        while let Some(hdl) = datasets.try_next().await? {
            let dataset = self.dataset_registry.get_dataset_by_handle(&hdl).await;

            let description = self.dataset_to_description(dataset).await?;

            for chunk in self.embeddings_chunker.chunk(description).await? {
                payloads.push(serde_json::json!({"id": hdl.id.to_string()}));
                chunks.push(chunk);
            }
        }

        tracing::debug!(?chunks, "Encoding chunks to embeddings");

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
