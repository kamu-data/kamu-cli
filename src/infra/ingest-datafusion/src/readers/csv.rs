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

        if let Some(v) = conf.separator.as_ref() {
            if v.as_bytes().len() > 1 {
                return Err("Csv.separator supports only single-character ascii values"
                    .int_err()
                    .into());
            }
        }
        match conf.encoding.as_ref().map(|s| s.as_str()) {
            None | Some("utf8") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.encoding: {}", v).int_err()),
        }?;
        match conf.quote.as_ref().map(|s| s.as_str()) {
            None | Some("\"") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.quote: {}", v).int_err()),
        }?;
        match conf.escape.as_ref().map(|s| s.as_str()) {
            None | Some("\\") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.escape: {}", v).int_err()),
        }?;
        match conf.comment.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.comment: {}", v).int_err()),
        }?;
        match conf.enforce_schema {
            None | Some(true) => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.enforceSchema: {}", v).int_err()),
        }?;
        match conf.ignore_leading_white_space {
            None | Some(false) => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.ignoreLeadingWhiteSpace: {}", v).int_err()),
        }?;
        match conf.ignore_trailing_white_space {
            None | Some(false) => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.ignoreTrailingWhiteSpace: {}", v).int_err()),
        }?;
        match conf.null_value.as_ref().map(|s| s.as_str()) {
            None | Some("") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.nullValue: {}", v).int_err()),
        }?;
        match conf.empty_value.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.emptyValue: {}", v).int_err()),
        }?;
        match conf.nan_value.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.nanValue: {}", v).int_err()),
        }?;
        match conf.positive_inf.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.positiveInf: {}", v).int_err()),
        }?;
        match conf.negative_inf.as_ref().map(|s| s.as_str()) {
            None => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.negativeInf: {}", v).int_err()),
        }?;
        match conf.date_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.dateFormat: {}", v).int_err()),
        }?;
        match conf.timestamp_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.timestampFormat: {}", v).int_err()),
        }?;
        match conf.multi_line {
            None | Some(false) => Ok(()),
            Some(v) => Err(format!("Unsupported Csv.multiLine: {}", v).int_err()),
        }?;

        let options = CsvReadOptions {
            has_header: conf.header.unwrap_or(false),
            delimiter: conf
                .separator
                .as_ref()
                .map(|s| s.as_bytes()[0])
                .unwrap_or(b','),
            schema: schema.as_ref(),
            schema_infer_max_records: if conf.infer_schema.unwrap_or(false) {
                Self::DEFAULT_INFER_SCHEMA_ROWS
            } else {
                0
            },
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            // TODO: PERF: Reader support compression, thus we could detect decompress step and
            // optimize the ingest plan to avoid writing uncompressed data to disc or having to
            // re-compress it.
            file_compression_type:
                datafusion::datasource::file_format::file_type::FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_csv(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
