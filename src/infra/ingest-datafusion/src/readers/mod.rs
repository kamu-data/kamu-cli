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
mod ndjson;
mod parquet;
mod shapefile;

pub use csv::*;
pub use geojson::*;
pub use ndjson::*;
pub use parquet::*;
pub use shapefile::*;

pub(crate) async fn output_schema_common(
    ctx: &datafusion::prelude::SessionContext,
    conf: &opendatafabric::ReadStep,
) -> Result<Option<datafusion::arrow::datatypes::Schema>, kamu_core::ingest::ReadError> {
    use internal_error::*;

    let Some(ddl_parts) = conf.schema() else {
        return Ok(None);
    };

    let ddl = ddl_parts.join(", ");

    let schema = kamu_data_utils::schema::parse::parse_ddl_to_arrow_schema(ctx, &ddl, true)
        .await
        .int_err()?;

    Ok(Some(schema))
}
