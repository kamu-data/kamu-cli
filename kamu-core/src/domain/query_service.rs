// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;

use datafusion::error::DataFusionError;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::DataFrame;
use opendatafabric::DatasetRef;
use thiserror::Error;

#[async_trait::async_trait]
pub trait QueryService: Send + Sync {
    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to the SQL query: `SELECT * FROM dataset ORDER BY offset DESC LIMIT N`
    async fn tail(
        &self,
        dataset_ref: &DatasetRef,
        num_records: u64,
    ) -> Result<DataFrame, QueryError>;

    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<DataFrame, QueryError>;

    /// Returns the schema of the given dataset, if it is already defined by this moment, None otherwise
    async fn get_schema(&self, dataset_ref: &DatasetRef) -> Result<Option<Type>, QueryError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub datasets: Vec<DatasetQueryOptions>,
}

#[derive(Debug, Clone)]
pub struct DatasetQueryOptions {
    pub dataset_ref: DatasetRef,
    /// Number of records that output requires (starting from latest entries)
    /// Setting this value allows to limit the number of part files examined.
    pub limit: Option<u64>,
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
