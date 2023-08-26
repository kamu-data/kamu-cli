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

pub struct ReaderJson {
    temp_path: PathBuf,
}

impl ReaderJson {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub fn new(temp_path: impl Into<PathBuf>) -> Self {
        Self {
            temp_path: temp_path.into(),
        }
    }

    fn convert_to_ndjson_blocking(
        in_path: &Path,
        sub_path: Option<&str>,
        out_path: &Path,
    ) -> Result<(), ReadError> {
        use std::io::Write;

        use serde_json::Value as JsonValue;

        let root: JsonValue =
            serde_json::from_reader(std::fs::File::open(in_path).int_err()?).int_err()?;

        let mut sub_value = &root;
        let mut path = Vec::new();
        if let Some(sub_path) = sub_path {
            for key in sub_path.split('.') {
                path.push(key);
                sub_value = &sub_value[key];

                if sub_value.is_null() {
                    return Err(format!("Sub-path not found: {}", path.join("."))
                        .int_err()
                        .into());
                }
            }
        }
        if path.is_empty() {
            path.push(".");
        }

        let Some(array) = sub_value.as_array() else {
            return Err(
                format!("Sub-path does not specify an array: {}", path.join("."))
                    .int_err()
                    .into(),
            );
        };

        let mut out_file = std::fs::File::create_new(out_path).int_err()?;

        for record in array {
            serde_json::to_writer(&mut out_file, record).int_err()?;
            writeln!(&mut out_file).int_err()?;
        }

        out_file.flush().int_err()?;
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderJson {
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
        let conf = match conf {
            ReadStep::Json(v) => v,
            _ => unreachable!(),
        };

        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes GeoJson into NdJson which DataFusion can read natively
        let sub_path = conf.sub_path.clone();
        let in_path = path.to_path_buf();
        let out_path = self.temp_path.clone();
        tokio::task::spawn_blocking(move || {
            Self::convert_to_ndjson_blocking(
                &in_path,
                sub_path.as_ref().map(|s| s.as_str()),
                &out_path,
            )
        })
        .await
        .int_err()??;

        let conf = ReadStep::NdJson(ReadStepNdJson {
            schema: conf.schema.clone(),
            date_format: conf.date_format.clone(),
            encoding: conf.encoding.clone(),
            timestamp_format: conf.timestamp_format.clone(),
        });

        ReaderNdJson::new().read(ctx, &self.temp_path, &conf).await
    }
}
