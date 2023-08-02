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
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderJsonLines {}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderJsonLines {
    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        let ReadStep::JsonLines(conf) = conf else {
            unreachable!()
        };

        let schema = match &conf.schema {
            Some(s) => Some(
                kamu_data_utils::schema::parse::parse_ddl_to_arrow_schema(ctx, s)
                    .await
                    .int_err()?,
            ),
            None => None,
        };

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
