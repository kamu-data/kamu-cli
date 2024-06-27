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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReaderCsv {
    ctx: SessionContext,
    schema: Option<Schema>,
    conf: ReadStepCsv,
}

impl ReaderCsv {
    const DEFAULT_INFER_SCHEMA_ROWS: usize = 1000;

    pub async fn new(ctx: SessionContext, conf: ReadStepCsv) -> Result<Self, ReadError> {
        Ok(Self {
            schema: super::from_ddl_schema(&ctx, &conf.schema).await?,
            ctx,
            conf,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderCsv {
    async fn input_schema(&self) -> Option<Schema> {
        self.schema.clone()
    }

    async fn read(&self, path: &Path) -> Result<DataFrame, ReadError> {
        // TODO: Move this to reader construction phase
        let delimiter = match &self.conf.separator {
            Some(v) if !v.is_empty() => {
                if v.as_bytes().len() > 1 {
                    return Err("Csv.separator supports only single-character ascii values"
                        .int_err()
                        .into());
                }
                v.as_bytes()[0]
            }
            _ => b',',
        };
        let quote = match &self.conf.quote {
            Some(v) if !v.is_empty() => {
                if v.as_bytes().len() > 1 {
                    Err(unsupported!(
                        "Csv.quote supports only single-character ascii values, got: {}",
                        v
                    ))
                } else {
                    Ok(v.as_bytes()[0])
                }
            }
            _ => Ok(b'"'),
        }?;
        let escape = match &self.conf.escape {
            Some(v) if !v.is_empty() => {
                if v.as_bytes().len() > 1 {
                    Err(unsupported!(
                        "Csv.escape supports only single-character ascii values, got: {}",
                        v
                    ))
                } else {
                    Ok(Some(v.as_bytes()[0]))
                }
            }
            _ => Ok(None),
        }?;
        match self.conf.encoding.as_deref() {
            None | Some("utf8") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.encoding: {}", v)),
        }?;
        match self.conf.null_value.as_deref() {
            None | Some("") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.nullValue: {}", v)),
        }?;
        match self.conf.date_format.as_deref() {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.dateFormat: {}", v)),
        }?;
        match self.conf.timestamp_format.as_deref() {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.timestampFormat: {}", v)),
        }?;

        let options = CsvReadOptions {
            schema: self.schema.as_ref(),
            delimiter,
            has_header: self.conf.header.unwrap_or(false),
            schema_infer_max_records: if self.conf.infer_schema.unwrap_or(false) {
                Self::DEFAULT_INFER_SCHEMA_ROWS
            } else {
                0
            },
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            quote,
            escape,
            table_partition_cols: Vec::new(),
            // TODO: PERF: Reader support compression, thus we could detect decompress step and
            // optimize the ingest plan to avoid writing uncompressed data to disc or having to
            // re-compress it.
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            file_sort_order: Vec::new(),
        };

        let df = self
            .ctx
            .read_csv(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
