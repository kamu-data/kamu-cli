// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use datafusion::datasource::file_format::file_type::FileCompressionType;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderNdGeoJson {
    temp_path: PathBuf,
}

impl ReaderNdGeoJson {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub fn new(temp_path: impl Into<PathBuf>) -> Self {
        Self {
            temp_path: temp_path.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderNdGeoJson {
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
        use std::io::prelude::*;

        use serde_json::Value as JsonValue;

        let schema = self.output_schema(ctx, conf).await?;

        let ReadStep::NdGeoJson(_) = conf else {
            unreachable!()
        };

        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes NdGeoJson into NdJson which DataFusion can read natively
        let mut out_file = std::fs::File::create_new(&self.temp_path).int_err()?;

        let in_file = std::fs::File::open(path).int_err()?;
        let mut reader = std::io::BufReader::new(in_file);
        let mut buffer = String::new();

        loop {
            if reader.read_line(&mut buffer).int_err()? == 0 {
                break;
            }
            let line = buffer.trim();

            if !line.is_empty() {
                let mut feature: serde_json::Map<String, JsonValue> =
                    serde_json::from_str(line).int_err()?;

                if feature["type"].as_str() != Some("Feature") {
                    return Err(format!(
                        "Expected Feature type but got {} instead",
                        feature["type"]
                    )
                    .int_err()
                    .into());
                }

                let mut record = match feature.remove("properties") {
                    Some(JsonValue::Object(v)) => Ok(v),
                    _ => Err("Invalid geojson".int_err()),
                }?;

                let geometry = match feature.remove("geometry") {
                    Some(JsonValue::Object(v)) => Ok(v),
                    _ => Err("Invalid geojson".int_err()),
                }?;

                let geom_str = serde_json::to_string(&geometry).int_err()?;
                record.insert("geometry".to_string(), JsonValue::String(geom_str));

                serde_json::to_writer(&mut out_file, &record).int_err()?;
                writeln!(&mut out_file).int_err()?;
            }

            buffer.clear();
        }

        out_file.flush().int_err()?;
        drop(out_file);

        let options = NdJsonReadOptions {
            file_extension: self
                .temp_path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or(""),
            table_partition_cols: Vec::new(),
            schema: schema.as_ref(),
            schema_infer_max_records: 1000,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_json(self.temp_path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
