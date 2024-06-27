// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use datafusion::arrow;
use datafusion::error::DataFusionError;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::{DataFrame, SessionContext};
use opendatafabric::*;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;
use crate::*;

// TODO: Support different engines and query dialects
#[async_trait::async_trait]
pub trait QueryService: Send + Sync {
    /// Creates an SQL session for the current user
    async fn create_session(&self) -> Result<SessionContext, CreateSessionError>;

    /// Returns the specified number of the latest records in the dataset
    /// This is equivalent to SQL query like:
    ///
    /// ```text
    /// select * from (
    ///   select
    ///     *
    ///   from dataset
    ///   order by offset desc
    ///   limit lim
    ///   offset skip
    /// )
    /// order by offset
    /// ```
    async fn tail(
        &self,
        dataset_ref: &DatasetRef,
        skip: u64,
        limit: u64,
    ) -> Result<DataFrame, QueryError>;

    /// Prepares an execution plan for the SQL statement and returns a
    /// [DataFrame] that can be used to get schema and data, and the state
    /// information that can be used for reproducibility.
    async fn sql_statement(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> Result<QueryResponse, QueryError>;

    /// Returns a reference-counted arrow schema of the given dataset, if it is
    /// already defined by this moment, `None` otherwise
    async fn get_schema(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError>;

    /// Returns parquet schema of the last data file in a given dataset, if any
    /// files were written, `None` otherwise
    async fn get_schema_parquet_file(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<Type>, QueryError>;

    // TODO: Introduce additional options that could be used to narrow down the
    // number of files we collect to construct the dataframe.
    //
    /// Returns a [DataFrame] representing the contents of an entire dataset
    async fn get_data(&self, dataset_ref: &DatasetRef) -> Result<DataFrame, QueryError>;

    /// Lists engines known to the system and recommended for use
    async fn get_known_engines(&self) -> Result<Vec<EngineDesc>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Maps the table names used in a query to globally unique dataset
    /// identifiers. If aliases are not provided - the table names will be
    /// treated as dataset references and resolved as normally in the context of
    /// the calling user. If at least one alias is specified - the name
    /// resolution will be disabled, thus you should either provide all aliases
    /// or none.
    pub aliases: Option<BTreeMap<String, DatasetID>>,
    /// Provides state information that should be used for query execution. This
    /// is used to achieve full reproducibility of queries as no matter what
    /// updates happen in the datasets - the query will only consider a specific
    /// subset of the data ledger.
    pub as_of_state: Option<QueryState>,
    /// Hints that can help the system to minimize metadata scanning. Be extra
    /// careful that your hints don't influence the actual result of the
    /// query, as they are not inlcuded in the [QueryState] and thus can
    /// ruin reproducibility if misused.
    pub hints: Option<BTreeMap<DatasetID, DatasetQueryHints>>,
}

#[derive(Debug, Clone, Default)]
pub struct DatasetQueryHints {
    /// Number of records that will be considered for this dataset (starting
    /// from latest entries) Setting this value allows engine to limit the
    /// number of part files examined, e.g. if limit is 100 and last data part
    /// file contains 150 records - only this file will be considered for the
    /// query and the rest of data will be completely ignored.
    pub last_records_to_consider: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct QueryResponse {
    /// A [DataFrame] that can be used to read schema and access the data. Note
    /// that the data frames are "lazy". They are a representation of a logical
    /// query plan. The actual query is executed only when you pull the
    /// resulting data from it.
    pub df: DataFrame,
    ///  The query state information that can be used for reproducibility.
    pub state: QueryState,
}

#[derive(Debug, Clone)]
pub struct QueryState {
    /// Last block hases of input datasets that were considered during the query
    /// planning
    pub inputs: BTreeMap<DatasetID, Multihash>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EngineDesc {
    /// A short name of the engine, e.g. "Spark", "Flink", "Datafusion"
    /// Intended for use in UI for quick engine identification and selection
    pub name: String,
    /// Language and dialect this engine is using for queries
    /// Indented for configuring correct code highlighting and completion
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
    SqlRisingWave,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum CreateSessionError {
    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

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
    Access(
        #[from]
        #[backtrace]
        AccessError,
    ),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset schema is not yet available: {dataset_ref}")]
pub struct DatasetSchemaNotAvailableError {
    pub dataset_ref: DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<GetDatasetError> for QueryError {
    fn from(v: GetDatasetError) -> Self {
        match v {
            GetDatasetError::NotFound(e) => Self::DatasetNotFound(e),
            GetDatasetError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<DatasetActionUnauthorizedError> for QueryError {
    fn from(v: DatasetActionUnauthorizedError) -> Self {
        match v {
            DatasetActionUnauthorizedError::Access(e) => Self::Access(e),
            DatasetActionUnauthorizedError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
