// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod csv;
mod geojson;
mod json;
mod ndgeojson;
mod ndjson;
mod parquet;
mod shapefile;

pub use csv::*;
pub use geojson::*;
pub use json::*;
pub use ndgeojson::*;
pub use ndjson::*;
pub use parquet::*;
pub use shapefile::*;

pub(crate) async fn from_ddl_schema(
    ctx: &datafusion::prelude::SessionContext,
    ddl_schema: Option<&Vec<String>>,
) -> Result<Option<datafusion::arrow::datatypes::Schema>, kamu_core::ingest::ReadError> {
    let Some(ddl_schema) = ddl_schema else {
        return Ok(None);
    };

    let ddl = ddl_schema.join(", ");

    let schema = odf::utils::schema::parse::parse_ddl_to_arrow_schema(ctx, &ddl, true).await?;

    Ok(Some(schema))
}

macro_rules! unsupported {
    ($($arg:tt)*) => {{
        let res = ::kamu_core::ingest::UnsupportedError::new(format!($($arg)*));
        res
    }}
}

macro_rules! bad_input {
    ($($arg:tt)*) => {{
        let res = ::kamu_core::ingest::BadInputError::new(format!($($arg)*));
        res
    }}
}

pub(crate) use {bad_input, unsupported};
