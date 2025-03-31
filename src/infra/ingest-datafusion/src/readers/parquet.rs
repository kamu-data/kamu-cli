// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use odf::utils::data::DataFrameExt;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReaderParquet {
    ctx: SessionContext,
    schema: Option<SchemaRef>,
}

impl ReaderParquet {
    pub async fn new(
        ctx: SessionContext,
        conf: odf::metadata::ReadStepParquet,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            schema: super::from_ddl_schema(&ctx, conf.schema.as_ref())
                .await?
                .map(Arc::new),
            ctx,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderParquet {
    async fn input_schema(&self) -> Option<SchemaRef> {
        self.schema.clone()
    }

    #[tracing::instrument(level = "info", name = "ReaderParquet::read", skip_all)]
    async fn read(&self, path: &Path) -> Result<DataFrameExt, ReadError> {
        let options = ParquetReadOptions {
            schema: self.schema.as_deref(),
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
            file_sort_order: Vec::new(),
        };

        let df = self
            .ctx
            .read_parquet(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df.into())
    }
}
