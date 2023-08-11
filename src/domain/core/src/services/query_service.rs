// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::error::DataFusionError;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::DataFrame;
use opendatafabric::DatasetRef;
use thiserror::Error;

use crate::*;

// TODO: Support different engines and query dialects
#[async_trait::async_trait]
pub trait QueryService: Send + Sync {
    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to the SQL query: `SELECT * FROM dataset ORDER BY
    /// offset DESC LIMIT N`
    async fn tail(
        &self,
        dataset_ref: &DatasetRef,
        skip: u64,
        limit: u64,
    ) -> Result<DataFrame, QueryError>;

    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<DataFrame, QueryError>;

    /// Returns the schema of the given dataset, if it is already defined by
    /// this moment, None otherwise
    async fn get_schema(&self, dataset_ref: &DatasetRef) -> Result<Option<Type>, QueryError>;

    /// Lists engines known to the system and recommended for use
    async fn get_known_engines(&self) -> Result<Vec<EngineDesc>, InternalError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub datasets: Vec<DatasetQueryOptions>,
}

#[derive(Debug, Clone)]
pub struct DatasetQueryOptions {
    pub dataset_ref: DatasetRef,
    /// Number of records that will be considered for this dataset (starting
    /// from latest entries) Setting this value allows engine to limit the
    /// number of part files examined, e.g. if limit is 100 and last data part
    /// file contains 150 records - only this file will be considered for the
    /// query and the rest of data will be completely ignored.
    pub last_records_to_consider: Option<u64>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineDesc {
    /// A short name of the engine, e.g. "Spark", "Flink"
    /// Intended for use in UI for quick engine identification and selection
    pub name: String,
    /// Language and dialect this engine is using for queries
    /// Indended for configuring correct code highlighting and completion
    pub dialect: QueryDialect,
    /// OCI image repository and a tag of the latest engine image, e.g.
    /// "ghcr.io/kamu-data/engine-datafusion:0.1.2"
    pub latest_image: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueryDialect {
    SqlSpark,
    SqlFlink,
    SqlDataFusion,
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    DatasetNotFound(
        #[from]
        #[backtrace]
        DatasetNotFoundError,
    ),
    #[error(transparent)]
    DatasetSchemaNotAvailable(
        #[from]
        #[backtrace]
        DatasetSchemaNotAvailableError,
    ),
    #[error(transparent)]
    DataFusionError(
        #[from]
        #[backtrace]
        DataFusionError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset schema is not yet available: {dataset_ref}")]
pub struct DatasetSchemaNotAvailableError {
    pub dataset_ref: DatasetRef,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl From<GetDatasetError> for QueryError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}
