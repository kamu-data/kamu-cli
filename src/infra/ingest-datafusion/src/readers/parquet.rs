// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderParquet {}

impl ReaderParquet {
    pub fn new() -> Self {
        Self {}
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderParquet {
    async fn read_schema(
        &self,
        ctx: &SessionContext,
        conf: &ReadStep,
    ) -> Result<Option<datafusion::arrow::datatypes::Schema>, ReadError> {
        super::read_schema_common(ctx, conf).await
    }

    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        let schema = self.read_schema(ctx, conf).await?;

        let ReadStep::Parquet(_) = conf else {
            unreachable!()
        };

        let options = ParquetReadOptions {
            schema: schema.as_ref(),
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
            file_sort_order: Vec::new(),
            insert_mode: datafusion::datasource::listing::ListingTableInsertMode::Error,
        };

        let df = ctx
            .read_parquet(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
