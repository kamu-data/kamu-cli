// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::datatypes::Schema;
use datafusion::common::DFSchema;
use datafusion::parquet::schema::types::Type;

pub fn arrow_schema_to_parquet_schema(arrow_schema: &Schema) -> Arc<Type> {
    let parquet_schema = datafusion::parquet::arrow::arrow_to_parquet_schema(arrow_schema).unwrap();
    parquet_schema.root_schema_ptr()
}

pub fn dataframe_schema_to_parquet_schema(df_schema: &DFSchema) -> Arc<Type> {
    arrow_schema_to_parquet_schema(&df_schema.into())
}
