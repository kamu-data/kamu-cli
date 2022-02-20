// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::queries::*;
use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use chrono::prelude::*;
use kamu::domain;
use opendatafabric as odf;
use opendatafabric::IntoDataStreamBlock;

pub(crate) struct DatasetMetadata {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMetadata {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    #[graphql(skip)]
    fn get_chain(&self, ctx: &Context<'_>) -> Result<Box<dyn domain::MetadataChain>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        Ok(dataset_reg.get_metadata_chain(&self.dataset_handle.as_local_ref())?)
    }

    /// Access to the temporal metadata chain of the dataset
    async fn chain(&self) -> MetadataChain {
        MetadataChain::new(self.dataset_handle.clone())
    }

    /// Last recorded watermark
    async fn current_watermark(&self, ctx: &Context<'_>) -> Result<Option<DateTime<Utc>>> {
        let chain = self.get_chain(ctx)?;
        Ok(chain
            .iter_blocks_ref(&domain::BlockRef::Head)
            .filter_map(|(_, b)| b.into_data_stream_block())
            .find_map(|b| b.event.output_watermark))
    }

    /// Latest data schema
    async fn current_schema(
        &self,
        ctx: &Context<'_>,
        format: Option<DataSchemaFormat>,
    ) -> Result<DataSchema> {
        let format = format.unwrap_or(DataSchemaFormat::Parquet);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        let schema = query_svc
            .get_schema(&self.dataset_handle.as_local_ref())
            .await?;

        Ok(DataSchema::from_parquet_schema(&schema, format)?)
    }

    /// Current upstream dependencies of a dataset
    async fn current_upstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        let summary = dataset_reg.get_summary(&self.dataset_handle.as_local_ref())?;
        Ok(summary
            .dependencies
            .into_iter()
            .map(|i| {
                Dataset::new(
                    Account::mock(),
                    odf::DatasetHandle::new(i.id.unwrap(), i.name),
                )
            })
            .collect())
    }

    /// Current downstream dependencies of a dataset
    async fn current_downstream_dependencies(&self, ctx: &Context<'_>) -> Result<Vec<Dataset>> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();

        // TODO: This is really slow
        Ok(dataset_reg
            .get_all_datasets()
            .filter(|hdl| hdl.id != self.dataset_handle.id)
            .map(|hdl| dataset_reg.get_summary(&hdl.as_local_ref()).unwrap())
            .filter(|sum| {
                sum.dependencies
                    .iter()
                    .any(|i| i.id.as_ref() == Some(&self.dataset_handle.id))
            })
            .map(|sum| Dataset::new(Account::mock(), odf::DatasetHandle::new(sum.id, sum.name)))
            .collect())
    }
}
