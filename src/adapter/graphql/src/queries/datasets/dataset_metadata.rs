// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
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
            .filter_map_ok(|(_, b)| b.event.new_watermark)
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
        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();

        match format {
            Some(DataSchemaFormat::ArrowJson) => {
                let schema_ref_opt = query_svc
                    .get_schema(&self.dataset_handle.as_local_ref())
                    .await
                    .int_err()?;

                match schema_ref_opt {
                    Some(schema_ref) => Ok(Option::from(DataSchema::from_arrow_schema(
                        schema_ref.as_ref(),
                    ))),
                    None => Ok(None),
                }
            }
            Some(DataSchemaFormat::Parquet) => {
                let schema = query_svc
                    .get_schema_parquet(&self.dataset_handle.as_local_ref())
                    .await
                    .int_err()?;

                let mut buf = Vec::new();
                kamu_data_utils::schema::format::write_schema_parquet(&mut buf, &schema.unwrap())
                    .unwrap();

                Ok(Option::from(DataSchema {
                    format: DataSchemaFormat::Parquet,
                    content: String::from_utf8(buf).unwrap(),
                }))
            }
            Some(DataSchemaFormat::ParquetJson) => {
                let schema = query_svc
                    .get_schema_parquet(&self.dataset_handle.as_local_ref())
                    .await
                    .int_err()?;

                let mut buf = Vec::new();
                kamu_data_utils::schema::format::write_schema_parquet_json(
                    &mut buf,
                    &schema.unwrap(),
                )
                .unwrap();

                Ok(Option::from(DataSchema {
                    format: DataSchemaFormat::ParquetJson,
                    content: String::from_utf8(buf).unwrap(),
                }))
            }
            _ => unimplemented!(),
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
        Ok(self
            .get_last_block_of_type::<odf::SetInfo>(ctx)
            .await?
            .map_or(
                SetInfo {
                    description: None,
                    keywords: None,
                },
                |b| b.event.into(),
            ))
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
