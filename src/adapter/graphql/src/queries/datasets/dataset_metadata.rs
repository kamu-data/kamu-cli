// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use futures::TryStreamExt;
use kamu_core::{self as domain, MetadataChainExt, TryStreamExtExt};
use opendatafabric as odf;
use opendatafabric::{AsTypedBlock, VariantOf};

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

    #[graphql(skip)]
    async fn get_last_block_of_type<T: VariantOf<odf::MetadataEvent>>(
        &self,
        ctx: &Context<'_>,
    ) -> Result<Option<odf::MetadataBlockTyped<T>>> {
        let dataset = self.get_dataset(ctx).await?;
        let block = dataset
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .filter_map_ok(|(_, b)| b.into_typed::<T>())
            .try_first()
            .await
            .int_err()?;
        Ok(block)
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_handle.clone())
    }

    /// Last recorded watermark
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let ds = self.get_dataset(ctx).await?;
        Ok(ds
            .as_metadata_chain()
            .iter_blocks_ref(&domain::BlockRef::Head)
            .filter_data_stream_blocks()
            .filter_map_ok(|(_, b)| b.event.output_watermark)
            .try_first()
            .await
            .int_err()?)
    }

    /// Latest data schema
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<Option<DataSchema>> {
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        let res_schema = query_svc
            .get_schema(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        match res_schema {
            Some(schema) => Ok(Some(DataSchema::from_parquet_schema(&schema, format)?)),
            None => Ok(None),
        }
    }

    /// Current upstream dependencies of a dataset
    async fn current_upstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        let dataset = self.get_dataset(ctx).await?;
        let summary = dataset
            .get_summary(domain::GetSummaryOpts::default())
            .await
            .int_err()?;

        let mut dependencies: Vec<_> = Vec::new();
        for input in summary.dependencies.into_iter() {
            let dataset_id = input.id.unwrap().clone();
            let dataset_handle = dataset_repo
                .resolve_dataset_ref(&dataset_id.as_local_ref())
                .await
                .int_err()?;
            dependencies.push(Dataset::new(
                Account::from_dataset_alias(ctx, &dataset_handle.alias),
                dataset_handle,
            ));
        }
        Ok(dependencies)
    }

    // TODO: Convert to collection
    /// Current downstream dependencies of a dataset
    async fn current_downstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_repo = from_catalog::<dyn domain::DatasetRepository>(ctx).unwrap();

        let downstream: Vec<_> = dataset_repo
            .get_downstream_dependencies(&self.dataset_handle.as_local_ref())
            .map_ok(|hdl| Dataset::new(Account::from_dataset_alias(ctx, &hdl.alias), hdl))
            .try_collect()
            .await?;

        Ok(downstream)
    }

    /// Deprecated - use `current_polling_source` instead
    async fn current_source(&self, ctx: &Context<'_>) -> Result<Option<SetPollingSource>> {
        self.current_polling_source(ctx).await
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

    /// Current polling source used by the root dataset
    async fn current_push_sources(&self, ctx: &Context<'_>) -> Result<Vec<AddPushSource>> {
        let push_ingest_svc = from_catalog::<dyn domain::PushIngestService>(ctx).unwrap();

        let push_sources = push_ingest_svc
            .get_active_push_sources(&self.dataset_handle.as_local_ref())
            .await
            .int_err()?;

        Ok(push_sources
            .into_iter()
            .map(|(_hash, block)| block.event.into())
            .collect())
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
        Ok(self
            .get_last_block_of_type::<odf::SetInfo>(ctx)
            .await?
            .map(|b| b.event.into())
            .unwrap_or(SetInfo {
                description: None,
                keywords: None,
            }))
    }

    /// Current readme file as discovered from attachments associated with the
    /// dataset
    async fn current_readme(&self, ctx: &Context<'_>) -> Result<Option<String>> {
        if let Some(attachments) = self
            .get_last_block_of_type::<odf::SetAttachments>(ctx)
            .await?
        {
            match attachments.event.attachments {
                odf::Attachments::Embedded(embedded) => Ok(embedded
                    .items
                    .into_iter()
                    .filter(|i| i.path == "README.md")
                    .map(|i| i.content)
                    .next()),
            }
        } else {
            Ok(None)
        }
    }

    /// Current license associated with the dataset
    async fn current_license(&self, ctx: &Context<'_>) -> Result<Option<SetLicense>> {
        Ok(self
            .get_last_block_of_type::<odf::SetLicense>(ctx)
            .await?
            .map(|b| b.event.into()))
    }

    /// Current vocabulary associated with the dataset
    async fn current_vocab(&self, ctx: &Context<'_>) -> Result<Option<SetVocab>> {
        Ok(self
            .get_last_block_of_type::<odf::SetVocab>(ctx)
            .await?
            .map(|b| b.event.into()))
    }
}
