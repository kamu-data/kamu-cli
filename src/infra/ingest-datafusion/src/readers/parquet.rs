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
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use odf::utils::data::DataFrameExt;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReaderParquet {
    ctx: SessionContext,
    conf: odf::metadata::ReadStepParquet,
    schema_arrow: Option<SchemaRef>,
}

impl ReaderParquet {
    pub fn new(
        ctx: SessionContext,
        conf: odf::metadata::ReadStepParquet,
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
impl Reader for ReaderParquet {
    fn schema(&self) -> Option<&odf::schema::DataSchema> {
        self.conf.schema.as_ref()
    }

    fn schema_arrow(&self) -> Option<&SchemaRef> {
        self.schema_arrow.as_ref()
    }

    #[tracing::instrument(level = "info", name = "ReaderParquet::read", skip_all)]
    async fn read(&self, path: &Path) -> Result<DataFrameExt, ReadError> {
        let options = ParquetReadOptions {
            schema: self.schema_arrow.as_deref(),
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            parquet_pruning: None,
            skip_metadata: None,
            file_sort_order: Vec::new(),
            file_decryption_properties: None,
            metadata_size_hint: None,
        };

        let df = self
            .ctx
            .read_parquet(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df.into())
    }
}
