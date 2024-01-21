// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::arrow::datatypes::Schema;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderNdJson {
    ctx: SessionContext,
    schema: Option<Schema>,
    conf: ReadStepNdJson,
}

impl ReaderNdJson {
    const DEFAULT_INFER_SCHEMA_ROWS: usize = 1000;

    pub async fn new(ctx: SessionContext, conf: ReadStepNdJson) -> Result<Self, ReadError> {
        Ok(Self {
            schema: super::from_ddl_schema(&ctx, &conf.schema).await?,
            ctx,
            conf,
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderNdJson {
    async fn input_schema(&self) -> Option<Schema> {
        self.schema.clone()
    }

    async fn read(&self, path: &Path) -> Result<DataFrame, ReadError> {
        // TODO: Move this to reader construction phase
        match self.conf.encoding.as_deref() {
            None | Some("utf8") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported NdJson.encoding: {}", v)),
        }?;
        match self.conf.date_format.as_deref() {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported NdJson.dateFormat: {}", v)),
        }?;
        match self.conf.timestamp_format.as_deref() {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported NdJson.timestampFormat: {}", v)),
        }?;

        let options = NdJsonReadOptions {
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            schema: self.schema.as_ref(),
            schema_infer_max_records: Self::DEFAULT_INFER_SCHEMA_ROWS,
            // TODO: PERF: Reader support compression, thus we could detect decompress step and
            // optimize the ingest plan to avoid writing uncompressed data to disc or having to
            // re-compress it.
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            file_sort_order: Vec::new(),
            infinite: false,
        };

        let df = self
            .ctx
            .read_json(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
