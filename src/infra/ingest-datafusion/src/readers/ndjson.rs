// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderNdJson {}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderNdJson {
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

        let conf = match conf.clone() {
            ReadStep::JsonLines(v) => ReadStepNdJson {
                schema: v.schema,
                date_format: v.date_format,
                encoding: v.encoding,
                primitives_as_string: v.primitives_as_string,
                timestamp_format: v.timestamp_format,
            },
            ReadStep::NdJson(v) => v,
            _ => unreachable!(),
        };

        match conf.encoding.as_ref().map(|s| s.as_str()) {
            None | Some("utf8") => Ok(()),
            Some(v) => Err(format!("Unsupported NdJson.encoding: {}", v).int_err()),
        }?;
        match conf.primitives_as_string {
            None | Some(false) => Ok(()),
            Some(v) => Err(format!("Unsupported NdJson.primitivesAsString: {}", v).int_err()),
        }?;
        match conf.date_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(format!("Unsupported NdJson.dateFormat: {}", v).int_err()),
        }?;
        match conf.timestamp_format.as_ref().map(|s| s.as_str()) {
            None | Some("rfc3339") => Ok(()),
            Some(v) => Err(format!("Unsupported NdJson.timestampFormat: {}", v).int_err()),
        }?;

        let options = NdJsonReadOptions {
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            schema: schema.as_ref(),
            schema_infer_max_records: 1000,
            // TODO: PERF: Reader support compression, thus we could detect decompress step and
            // optimize the ingest plan to avoid writing uncompressed data to disc or having to
            // re-compress it.
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_json(path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
