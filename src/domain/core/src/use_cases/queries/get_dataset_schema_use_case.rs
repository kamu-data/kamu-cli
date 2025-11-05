// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::QueryError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait GetDatasetSchemaUseCase: Send + Sync {
    /// Returns an ODF schema of the given dataset as specified in the metadata
    /// chain. Returns `None` if schema was not yet defined.
    async fn get_schema(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Arc<odf::schema::DataSchema>>, QueryError>;

    /// Returns Arrow schema of the last data file in a given dataset, if any
    /// files were written, `None` otherwise. Unlike `get_schema` that uses
    /// schema from metadata chain, this will return the raw Arrow schema of
    /// data as seen by the query engine.
    async fn get_last_data_chunk_schema_arrow(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<datafusion::arrow::datatypes::SchemaRef>, QueryError>;

    /// Returns parquet schema of the last data file in a given dataset, if any
    /// files were written, `None` otherwise
    async fn get_last_data_chunk_schema_parquet(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<datafusion::parquet::schema::types::Type>, QueryError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
