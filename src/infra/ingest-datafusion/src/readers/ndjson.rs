// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use odf::utils::data::DataFrameExt;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReaderNdJson {
    ctx: SessionContext,
    conf: odf::metadata::ReadStepNdJson,
    schema_arrow: Option<SchemaRef>,
}

impl ReaderNdJson {
    const DEFAULT_INFER_SCHEMA_ROWS: usize = 1000;

    pub fn new(
        ctx: SessionContext,
        conf: odf::metadata::ReadStepNdJson,
        to_arrow_settings: &odf::schema::ToArrowSettings,
    ) -> Result<Self, ReadError> {
        assert!(
            conf.ddl_schema.is_none(),
            "DDL schema must be normalized before into ODF before creating a reader"
        );

        let schema_arrow = conf
            .schema
            .as_ref()
            .map(|s| s.to_arrow(to_arrow_settings))
            .transpose()
            .int_err()?
            .map(std::sync::Arc::new);

        Ok(Self {
            ctx,
            conf,
            schema_arrow,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderNdJson {
    fn schema(&self) -> Option<&odf::schema::DataSchema> {
        self.conf.schema.as_ref()
    }

    fn schema_arrow(&self) -> Option<&SchemaRef> {
        self.schema_arrow.as_ref()
    }

    #[tracing::instrument(level = "info", name = "ReaderNdJson::read", skip_all)]
    async fn read(&self, path: &Path) -> Result<DataFrameExt, ReadError> {
        // TODO: Move this to reader construction phase
        match self.conf.encoding() {
            "utf8" => Ok(()),
            v => Err(unsupported!("Unsupported NdJson.encoding: {}", v)),
        }?;
        match self.conf.date_format() {
            "rfc3339" => Ok(()),
            v => Err(unsupported!("Unsupported NdJson.dateFormat: {}", v)),
        }?;
        match self.conf.timestamp_format() {
            "rfc3339" => Ok(()),
            v => Err(unsupported!("Unsupported NdJson.timestampFormat: {}", v)),
        }?;

        let options = JsonReadOptions {
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            schema: self.schema_arrow.as_deref(),
            schema_infer_max_records: Self::DEFAULT_INFER_SCHEMA_ROWS,
            // TODO: PERF: Reader support compression, thus we could detect decompress step and
            // optimize the ingest plan to avoid writing uncompressed data to disc or having to
            // re-compress it.
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            file_sort_order: Vec::new(),
            infinite: false,
            newline_delimited: true,
        };

        let df = self
            .ctx
            .read_json(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df.into())
    }
}
