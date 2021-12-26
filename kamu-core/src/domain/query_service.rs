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
use opendatafabric::DatasetRefLocal;
use std::sync::Arc;
use thiserror::Error;

use super::DomainError;

pub trait QueryService: Send + Sync {
    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to the SQL query: `SELECT * FROM dataset ORDER BY offset DESC LIMIT N`
    fn tail(
        &self,
        dataset_ref: &DatasetRefLocal,
        num_records: u64,
    ) -> Result<Arc<dyn DataFrame>, QueryError>;

    fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<Arc<dyn DataFrame>, QueryError>;

    fn get_schema(&self, dataset_ref: &DatasetRefLocal) -> Result<Type, QueryError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub datasets: Vec<DatasetQueryOptions>,
}

#[derive(Debug, Clone)]
pub struct DatasetQueryOptions {
    pub dataset_ref: DatasetRefLocal,
    /// Number of records that output requires (starting from latest entries)
    /// Setting this value allows to limit the number of part files examined.
    pub limit: Option<u64>,
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Domain error: {0}")]
    DomainError(#[from] DomainError),
    #[error("{0}")]
    DataFusionError(#[from] DataFusionError),
    #[error("{0}")]
    InternalError(#[source] BoxedError),
}

impl QueryError {
    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::InternalError(e.into())
    }
}

impl From<std::io::Error> for QueryError {
    fn from(e: std::io::Error) -> Self {
        Self::internal(e)
    }
}
