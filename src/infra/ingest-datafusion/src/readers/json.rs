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

pub struct ReaderJson {
    sub_path: Option<String>,
    temp_path: PathBuf,
    inner: ReaderNdJson,
}

impl ReaderJson {
    // TODO: This is an ugly API that leaves it to the caller to clean up our temp
    // file mess. Ideally we should not produce any temp files at all and stream in
    // all data.
    pub async fn new(
        ctx: SessionContext,
        conf: odf::metadata::ReadStepJson,
        temp_path: impl Into<PathBuf>,
    ) -> Result<Self, ReadError> {
        let inner_conf = odf::metadata::ReadStepNdJson {
            schema: conf.schema,
            date_format: conf.date_format,
            encoding: conf.encoding,
            timestamp_format: conf.timestamp_format,
        };

        Ok(Self {
            sub_path: conf.sub_path,
            temp_path: temp_path.into(),
            inner: ReaderNdJson::new(ctx, inner_conf).await?,
        })
    }

    #[tracing::instrument(level = "info", name = "ReaderJson::convert_to_ndjson", skip_all)]
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
                    return Err(bad_input!("Sub-path not found: {}", path.join(".")).into());
                }
            }
        }
        if path.is_empty() {
            path.push(".");
        }

        let Some(array) = sub_value.as_array() else {
            return Err(
                bad_input!("Sub-path does not specify an array: {}", path.join(".")).into(),
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Reader for ReaderJson {
    async fn input_schema(&self) -> Option<SchemaRef> {
        self.inner.input_schema().await
    }

    #[tracing::instrument(level = "info", name = "ReaderJson::read", skip_all)]
    async fn read(&self, path: &Path) -> Result<DataFrameExt, ReadError> {
        // TODO: PERF: This is a temporary, highly inefficient implementation that
        // re-encodes GeoJson into NdJson which DataFusion can read natively
        let sub_path = self.sub_path.clone();
        let in_path = path.to_path_buf();
        let out_path = self.temp_path.clone();
        tokio::task::spawn_blocking(move || {
            Self::convert_to_ndjson_blocking(&in_path, sub_path.as_deref(), &out_path)
        })
        .await
        .int_err()??;

        self.inner.read(&self.temp_path).await
    }
}
