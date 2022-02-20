// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::scalars::*;
use crate::utils::*;

use async_graphql::*;
use kamu::domain;
use opendatafabric as odf;

pub(crate) struct DatasetData {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetData {
    const DEFAULT_TAIL_LIMIT: u64 = 20;

    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Total number of records in this dataset
    async fn num_records_total(&self, ctx: &Context<'_>) -> Result<u64> {
        let cat = ctx.data::<dill::Catalog>().unwrap();
        let dataset_reg = cat.get_one::<dyn domain::DatasetRegistry>().unwrap();
        let summary = dataset_reg.get_summary(&self.dataset_handle.as_local_ref())?;
        Ok(summary.num_records)
    }

    /// An estimated size of data on disk not accounting for replication or caching
    async fn estimated_size(&self, ctx: &Context<'_>) -> Result<u64> {
        let dataset_reg = from_catalog::<dyn domain::DatasetRegistry>(ctx).unwrap();
        let summary = dataset_reg.get_summary(&self.dataset_handle.as_local_ref())?;
        Ok(summary.data_size)
    }

    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to the SQL query: `SELECT * FROM dataset ORDER BY event_time DESC LIMIT N`
    async fn tail(
        &self,
        ctx: &Context<'_>,
        num_records: Option<u64>,
        format: Option<DataSliceFormat>,
    ) -> Result<DataSlice> {
        // TODO: Default to JsonSoA format once implemented
        let format = format.unwrap_or(DataSliceFormat::Json);
        let num_records = num_records.unwrap_or(Self::DEFAULT_TAIL_LIMIT);

        let query_svc = from_catalog::<dyn domain::QueryService>(ctx).unwrap();
        let df = query_svc
            .tail(&self.dataset_handle.as_local_ref(), num_records)
            .await?;

        let record_batches = df.collect().await?;
        Ok(DataSlice::from_records(&record_batches, format)?)
    }
}
