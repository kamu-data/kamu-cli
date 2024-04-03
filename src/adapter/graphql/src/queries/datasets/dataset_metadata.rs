// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use kamu_core::{
    self as domain,
    MetadataChainExt,
    SearchSetAttachmentsVisitor,
    SearchSetInfoVisitor,
    SearchSetLicenseVisitor,
    SearchSetVocabVisitor,
};
use opendatafabric as odf;

use crate::prelude::*;
use crate::queries::*;

pub struct DatasetMetadata {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMetadata {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    async fn get_dataset(&self, ctx: &Context<'_>) -> Result<std::sync::Arc<dyn domain::Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let dataset = dataset_repo
            .get_dataset(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;
        Ok(dataset)
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_handle.clone())
    }

    /// Last recorded watermark
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .last_data_block()
            .await
            .int_err()?
            .into_block()
            .and_then(|b| b.event.new_watermark))
    }

    /// Latest data schema
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<Option<DataSchema>> {
        // TODO: Default to Arrow eventually
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();

        if let Some(schema) = query_svc
            .get_schema(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?
        {
            Ok(Option::from(DataSchema::from_arrow_schema(
                schema.as_ref(),
                format,
            )))
        } else {
            Ok(None)
        }
    }

    /// Current upstream dependencies of a dataset
    async fn current_upstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dependency_graph_service =
            from_catalog::<dyn domain::DependencyGraphService>(ctx).unwrap();

        use tokio_stream::StreamExt;
        let upstream_dataset_ids: Vec<_> = dependency_graph_service
            .get_upstream_dependencies(&self.dataset_handle.id)
            .await
            .int_err()?
            .collect()
            .await;

        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let mut upstream = Vec::with_capacity(upstream_dataset_ids.len());
        for upstream_dataset_id in upstream_dataset_ids {
            let hdl = dataset_repo
                .resolve_dataset_ref(&upstream_dataset_id.as_local_ref())
                .await
                .int_err()?;
            upstream.push(Dataset::new(
                Account::from_dataset_alias(ctx, &hdl.alias),
                hdl,
            ));
        }

        Ok(upstream)
    }

    // TODO: Convert to collection
    /// Current downstream dependencies of a dataset
    async fn current_downstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dependency_graph_service =
            from_catalog::<dyn domain::DependencyGraphService>(ctx).unwrap();

        use tokio_stream::StreamExt;
        let downstream_dataset_ids: Vec<_> = dependency_graph_service
            .get_downstream_dependencies(&self.dataset_handle.id)
            .await
            .int_err()?
            .collect()
            .await;

        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();
        let mut downstream = Vec::with_capacity(downstream_dataset_ids.len());
        for downstream_dataset_id in downstream_dataset_ids {
            let hdl = dataset_repo
                .resolve_dataset_ref(&downstream_dataset_id.as_local_ref())
                .await
                .int_err()?;
            downstream.push(Dataset::new(
                Account::from_dataset_alias(ctx, &hdl.alias),
                hdl,
            ));
        }

        Ok(downstream)
    }

    /// Current polling source used by the root dataset
    async fn current_polling_source(&self, ctx: &Context<'_>) -> Result<Option<SetPollingSource>> {
        let polling_ingest_svc = from_catalog::<dyn domain::PollingIngestService>(ctx).unwrap();

        let source = polling_ingest_svc
            .get_active_polling_source(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        Ok(source.map(|(_hash, block)| block.event.into()))
    }

    /// Current push sources used by the root dataset
    async fn current_push_sources(&self, ctx: &Context<'_>) -> Result<Vec<AddPushSource>> {
        let push_ingest_svc = from_catalog::<dyn domain::PushIngestService>(ctx).unwrap();

        let mut push_sources: Vec<AddPushSource> = push_ingest_svc
            .get_active_push_sources(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?
            .into_iter()
            .map(|(_hash, block)| block.event.into())
            .collect();

        push_sources.sort_by(|a, b| a.source_name.cmp(&b.source_name));

        Ok(push_sources)
    }

    /// Current transformation used by the derivative dataset
    async fn current_transform(&self, ctx: &Context<'_>) -> Result<Option<SetTransform>> {
        let transform_svc = from_catalog::<dyn domain::TransformService>(ctx).unwrap();

        let source = transform_svc
            .get_active_transform(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        Ok(source.map(|(_hash, block)| block.event.into()))
    }

    /// Current descriptive information about the dataset
    async fn current_info(&self, ctx: &Context<'_>) -> Result<SetInfo> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetInfoVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map_or(
                SetInfo {
                    description: None,
                    keywords: None,
                },
                Into::into,
            ))
    }

    /// Current readme file as discovered from attachments associated with the
    /// dataset
    async fn current_readme(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetAttachmentsVisitor::new())
            .await
            .int_err()?
            .into_event()
            .and_then(|e| {
                let odf::Attachments::Embedded(at) = e.attachments;

                at.items
                    .into_iter()
                    .filter(|i| i.path == "README.md")
                    .map(|i| i.content)
                    .next()
            }))
    }

    /// Current license associated with the dataset
    async fn current_license(&self, ctx: &Context<'_>) -> Result<Option<SetLicense>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetLicenseVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }

    /// Current vocabulary associated with the dataset
    async fn current_vocab(&self, ctx: &Context<'_>) -> Result<Option<SetVocab>> {
        let dataset = self.get_dataset(ctx).await?;

        Ok(dataset
            .as_metadata_chain()
            .accept_one(SearchSetVocabVisitor::new())
            .await
            .int_err()?
            .into_event()
            .map(Into::into))
    }
}
