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

pub struct ReaderCsv {}

impl ReaderCsv {
    const DEFAULT_INFER_SCHEMA_ROWS: usize = 1000;

    pub fn new() -> Self {
        Self {}
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderCsv {
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

        let ReadStep::Csv(conf) = conf else {
            unreachable!()
        };

        let delimiter = match &conf.separator {
            Some(v) if v.len() > 0 => {
                if v.as_bytes().len() > 1 {
                    return Err("Csv.separator supports only single-character ascii values"
                        .int_err()
                        .into());
                }
                v.as_bytes()[0]
            }
            _ => b',',
        };
        let quote = match &conf.quote {
            Some(v) if v.len() > 0 => {
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
        let escape = match &conf.escape {
            Some(v) if v.len() > 0 => {
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
        match conf.encoding.as_ref().map(|s| s.as_str()) {
            None | Some("utf8") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.encoding: {}", v)),
        }?;
        match conf.comment.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.comment: {}", v)),
        }?;
        match conf.enforce_schema {
            None | Some(true) => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.enforceSchema: {}", v)),
        }?;
        match conf.ignore_leading_white_space {
            None | Some(false) => Ok(()),
            Some(v) => Err(unsupported!(
                "Unsupported Csv.ignoreLeadingWhiteSpace: {}",
                v
            )),
        }?;
        match conf.ignore_trailing_white_space {
            None | Some(false) => Ok(()),
            Some(v) => Err(unsupported!(
                "Unsupported Csv.ignoreTrailingWhiteSpace: {}",
                v
            )),
        }?;
        match conf.null_value.as_ref().map(|s| s.as_str()) {
            None | Some("") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.nullValue: {}", v)),
        }?;
        match conf.empty_value.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.emptyValue: {}", v)),
        }?;
        match conf.nan_value.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.nanValue: {}", v)),
        }?;
        match conf.positive_inf.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.positiveInf: {}", v)),
        }?;
        match conf.negative_inf.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.negativeInf: {}", v)),
        }?;
        match conf.date_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.dateFormat: {}", v)),
        }?;
        match conf.timestamp_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.timestampFormat: {}", v)),
        }?;
        match conf.multi_line {
            None | Some(false) => Ok(()),
            Some(v) => Err(unsupported!("Unsupported Csv.multiLine: {}", v)),
        }?;

        let options = CsvReadOptions {
            schema: schema.as_ref(),
            delimiter,
            has_header: conf.header.unwrap_or(false),
            schema_infer_max_records: if conf.infer_schema.unwrap_or(false) {
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
            file_compression_type: datafusion::common::FileCompressionType::UNCOMPRESSED,
            file_sort_order: Vec::new(),
            insert_mode: datafusion::datasource::listing::ListingTableInsertMode::Error,
            infinite: false,
        };

        let df = ctx
            .read_csv(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
