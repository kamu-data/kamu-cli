// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use datafusion::prelude::*;
use internal_error::*;
use kamu_core::ingest::ReadError;
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderGeoJson {
    temp_path: PathBuf,
}

impl ReaderGeoJson {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub fn new(temp_path: impl Into<PathBuf>) -> Self {
        Self {
            temp_path: temp_path.into(),
        }
    }

    fn convert_to_ndjson_blocking(in_path: &Path, out_path: &Path) -> Result<(), ReadError> {
        use std::io::Write;

        use serde_json::Value as JsonValue;

        let mut feature_col: serde_json::Map<String, JsonValue> =
            serde_json::from_reader(std::fs::File::open(in_path).int_err()?).int_err()?;

        if feature_col["type"].as_str() != Some("FeatureCollection") {
            return Err(bad_input!(
                "Expected FeatureCollection type but got {} instead",
                feature_col["type"]
            )
            .into());
        }

        let features = match feature_col.remove("features") {
            Some(JsonValue::Array(v)) => Ok(v),
            _ => Err(bad_input!("Invalid geojson")),
        }?;

        let mut out_file = std::fs::File::create_new(out_path).int_err()?;

        for feature in features {
            let JsonValue::Object(mut feature) = feature else {
                return Err(bad_input!("Invalid geojson").into());
            };

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

        out_file.flush().int_err()?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderGeoJson {
    async fn read_schema(
        &self,
        ctx: &SessionContext,
        conf: &ReadStep,
    ) -> Result<Option<datafusion::arrow::datatypes::Schema>, ReadError> {
        super::read_schema_common(ctx, conf).await
    }

    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        let ReadStep::GeoJson(conf) = conf else {
            unreachable!()
        };

        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes GeoJson into NdJson which DataFusion can read natively
        let in_path = path.to_path_buf();
        let out_path = self.temp_path.clone();
        tokio::task::spawn_blocking(move || Self::convert_to_ndjson_blocking(&in_path, &out_path))
            .await
            .int_err()??;

        let conf = ReadStep::NdJson(ReadStepNdJson {
            schema: conf.schema.clone(),
            date_format: None,
            encoding: None,
            timestamp_format: None,
        });

        ReaderNdJson::new().read(ctx, &self.temp_path, &conf).await
    }
}
