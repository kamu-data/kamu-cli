// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use odf::utils::data::DataFrameExt;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ReaderNdGeoJson {
    temp_path: PathBuf,
    inner: ReaderNdJson,
}

impl ReaderNdGeoJson {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub async fn new(
        ctx: SessionContext,
        conf: odf::metadata::ReadStepNdGeoJson,
        temp_path: impl Into<PathBuf>,
    ) -> Result<Self, ReadError> {
        let inner_conf = odf::metadata::ReadStepNdJson {
            schema: conf.schema,
            date_format: None,
            encoding: None,
            timestamp_format: None,
        };

        Ok(Self {
            inner: ReaderNdJson::new(ctx, inner_conf).await?,
            temp_path: temp_path.into(),
        })
    }

    #[tracing::instrument(level = "info", name = "ReaderNdGeoJson::convert_to_ndjson", skip_all)]
    fn convert_to_ndjson_blocking(in_path: &Path, out_path: &Path) -> Result<(), ReadError> {
        use std::io::prelude::*;

        use serde_json::Value as JsonValue;

        let in_file = std::fs::File::open(in_path).int_err()?;
        let mut out_file = std::fs::File::create_new(out_path).int_err()?;

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
                    return Err(bad_input!(
                        "Expected Feature type but got {} instead",
                        feature["type"]
                    )
                    .into());
                }

                let mut record = match feature.remove("properties") {
                    Some(JsonValue::Object(v)) => Ok(v),
                    _ => Err(bad_input!("Invalid geojson")),
                }?;

                let geometry = match feature.remove("geometry") {
                    Some(JsonValue::Object(v)) => Ok(v),
                    _ => Err(bad_input!("Invalid geojson")),
                }?;

                let geom_str = serde_json::to_string(&geometry).int_err()?;
                record.insert("geometry".to_string(), JsonValue::String(geom_str));

                serde_json::to_writer(&mut out_file, &record).int_err()?;
                writeln!(&mut out_file).int_err()?;
            }

            buffer.clear();
        }

        out_file.flush().int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderNdGeoJson {
    async fn input_schema(&self) -> Option<SchemaRef> {
        self.inner.input_schema().await
    }

    #[tracing::instrument(level = "info", name = "ReaderNdGeoJson::read", skip_all)]
    async fn read(&self, path: &Path) -> Result<DataFrameExt, ReadError> {
        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes NdGeoJson into NdJson which DataFusion can read natively
        let in_path = path.to_path_buf();
        let out_path = self.temp_path.clone();
        tokio::task::spawn_blocking(move || Self::convert_to_ndjson_blocking(&in_path, &out_path))
            .await
            .int_err()??;

        self.inner.read(&self.temp_path).await
    }
}
