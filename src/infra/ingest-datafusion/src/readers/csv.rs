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
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderCsv {}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderCsv {
    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        let ReadStep::Csv(csv) = conf else {
            unreachable!()
        };

        let schema = match &csv.schema {
            Some(s) => Some(
                kamu_data_utils::schema::parse::parse_ddl_schema_to_arrow(ctx, s)
                    .await
                    .int_err()?,
            ),
            None => None,
        };

        let csv_options = CsvReadOptions {
            has_header: csv.header.unwrap_or(false),
            delimiter: b',',
            schema: schema.as_ref(),
            schema_infer_max_records: 1000,
            file_extension: path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            file_compression_type:
                datafusion::datasource::file_format::file_type::FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_csv(path.to_str().unwrap(), csv_options)
            .await
            .int_err()?;

        Ok(df)
    }
}
