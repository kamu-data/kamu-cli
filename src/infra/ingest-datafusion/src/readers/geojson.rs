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
use opendatafabric::*;

use crate::*;

///////////////////////////////////////////////////////////////////////////////

pub struct ReaderGeoJson {
    temp_dir: PathBuf,
}

impl ReaderGeoJson {
    pub fn new(temp_dir: impl Into<PathBuf>) -> Self {
        Self {
            temp_dir: temp_dir.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderGeoJson {
    async fn read(
        &self,
        ctx: &SessionContext,
        path: &Path,
        conf: &ReadStep,
    ) -> Result<DataFrame, ReadError> {
        use std::io::Write;

        use serde_json::Value as JsonValue;

        let schema = self.output_schema(ctx, conf).await?;

        let ReadStep::GeoJson(_) = conf else {
            unreachable!()
        };

        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes GeoJson into NdJson which DataFusion can read natively
        let json: JsonValue =
            serde_json::from_reader(std::fs::File::open(path).int_err()?).int_err()?;

        let features = match json["type"].as_str() {
            Some("FeatureCollection") => json["features"]
                .as_array()
                .ok_or_else(|| "features key not found".int_err()),
            Some(typ) => {
                Err(format!("Expected FeatureCollection type but got {} instead", typ).int_err())
            }
            _ => Err("Object doesn't look like a FeatureCollection".int_err()),
        }?;

        let temp_path = self.temp_dir.join("temp.json");
        let mut file = std::fs::File::create_new(&temp_path).int_err()?;

        for feature in features {
            let mut record = feature["properties"]
                .as_object()
                .ok_or_else(|| "Invalid geojson".int_err())?
                .clone();

            let geom_str = serde_json::to_string(&feature["geometry"]).int_err()?;
            record.insert("geometry".to_string(), JsonValue::String(geom_str));

            serde_json::to_writer(&mut file, &record).int_err()?;
            writeln!(&mut file).int_err()?;
        }

        file.flush().int_err()?;

        let options = NdJsonReadOptions {
            file_extension: temp_path.extension().and_then(|s| s.to_str()).unwrap_or(""),
            table_partition_cols: Vec::new(),
            schema: schema.as_ref(),
            schema_infer_max_records: 1000,
            file_compression_type: FileCompressionType::UNCOMPRESSED,
            infinite: false,
        };

        let df = ctx
            .read_json(temp_path.to_str().unwrap(), options)
            .await
            .int_err()?;

        Ok(df)
    }
}
