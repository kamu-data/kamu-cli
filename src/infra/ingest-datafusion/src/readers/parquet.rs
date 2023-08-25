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
    async fn output_schema(
        &self,
        ctx: &SessionContext,
        conf: &ReadStep,
    ) -> Result<Option<datafusion::arrow::datatypes::Schema>, ReadError> {
        super::output_schema_common(ctx, conf).await
    }

    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        let schema = self.output_schema(ctx, conf).await?;

        let ReadStep::Parquet(_) = conf else {
            unreachable!()
        };

        let options = ParquetReadOptions {
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
        };

        let df = ctx
            .read_parquet(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        let df = if let Some(schema) = schema {
            tracing::debug!(
                orig_schema = ?df.schema(),
                target_schema = ?schema,
                "Performing read coercion of the parquet data",
            );

            df.select(
                schema
                    .fields()
                    .iter()
                    .map(|f| cast(col(f.name()), f.data_type().clone()).alias(f.name()))
                    .collect(),
            )
            .int_err()?
        } else {
            df
        };

        Ok(df)
    }
}
