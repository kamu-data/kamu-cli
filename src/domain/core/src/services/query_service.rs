// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::collections::BTreeMap;

use datafusion::arrow;
use datafusion::parquet::schema::types::Type;
use datafusion::prelude::{DataFrame, SessionContext};
use internal_error::InternalError;
use thiserror::Error;

use crate::auth::DatasetActionUnauthorizedError;

// TODO: Support different engines and query dialects
#[cfg_attr(feature = "testing", mockall::automock)]
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
        dataset_ref: &odf::DatasetRef,
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
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<arrow::datatypes::SchemaRef>, QueryError>;

    /// Returns parquet schema of the last data file in a given dataset, if any
    /// files were written, `None` otherwise
    async fn get_schema_parquet_file(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<Type>, QueryError>;

    // TODO: Introduce additional options that could be used to narrow down the
    // number of files we collect to construct the dataframe.
    //
    /// Returns a [DataFrame] representing the contents of an entire dataset
    async fn get_data(&self, dataset_ref: &odf::DatasetRef) -> Result<DataFrame, QueryError>;

    /// Lists engines known to the system and recommended for use
    async fn get_known_engines(&self) -> Result<Vec<EngineDesc>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    /// Options for datasets used as query inputs. If options for at least one
    /// dataset are specified the name resolution will be disabled, so you
    /// should either provide them for all datasets or none. If no options are
    /// provided the table names in the query will be treated as dataset
    /// references and resolved as normally in the context of the calling
    /// user.
    pub input_datasets: BTreeMap<odf::DatasetID, QueryOptionsDataset>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryOptionsDataset {
    /// Associates a table name in a query with the globally unique dataset
    /// identifier.
    pub alias: String,
    /// Last block hash of an input dataset that should be used for query
    /// execution. This is used to achieve full reproducibility of queries
    /// as no matter what updates happen in the datasets - the query will
    /// only consider a specific subset of the data ledger.
    pub block_hash: Option<odf::Multihash>,
    /// Hints that can help the system to minimize metadata scanning. Be extra
    /// careful that your hints don't influence the actual result of the
    /// query, as they are not inlcuded in the [`QueryState`] and thus can
    /// ruin reproducibility if misused.
    pub hints: Option<DatasetQueryHints>,
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
    /// A [`DataFrame`] that can be used to read schema and access the data.
    /// Note that the data frames are "lazy". They are a representation of a
    /// logical query plan. The actual query is executed only when you pull
    /// the resulting data from it.
    pub df: DataFrame,
    ///  The query state information that can be used for reproducibility.
    pub state: QueryState,
}

#[derive(Debug, Clone)]
pub struct QueryState {
    /// State of the input datasets used in the query
    pub input_datasets: BTreeMap<odf::DatasetID, QueryStateDataset>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryStateDataset {
    /// Alias of the dataset used in the query
    pub alias: String,
    /// Last block hash that was considered during the
    /// query planning
    pub block_hash: odf::Multihash,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ::serde::Serialize, ::serde::Deserialize)]
#[cfg_attr(feature = "utoipa", derive(utoipa::ToSchema))]
pub enum QueryDialect {
    SqlDataFusion,
    SqlFlink,
    SqlRisingWave,
    SqlSpark,
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
        odf::metadata::AccessError,
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
        odf::DatasetNotFoundError,
    ),

    #[error(transparent)]
    DatasetBlockNotFound(
        #[from]
        #[backtrace]
        DatasetBlockNotFoundError,
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
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This error returned only when the caller provides an explicit block hash to
/// query via [`QueryOptionsDataset`]
#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset {dataset_id} does not have a block {block_hash}")]
pub struct DatasetBlockNotFoundError {
    pub dataset_id: odf::DatasetID,
    pub block_hash: odf::Multihash,
}

impl DatasetBlockNotFoundError {
    pub fn new(dataset_id: odf::DatasetID, block_hash: odf::Multihash) -> Self {
        Self {
            dataset_id,
            block_hash,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Wraps [`datafusion::error::DataFusionError`] error to attach a backtrace at
/// the earliest point
#[derive(Error, Debug)]
#[error("DataFusion error")]
pub struct DataFusionError {
    #[from]
    pub source: datafusion::error::DataFusionError,
    pub backtrace: Backtrace,
}

impl From<datafusion::error::DataFusionError> for QueryError {
    fn from(value: datafusion::error::DataFusionError) -> Self {
        Self::DataFusionError(DataFusionError {
            source: value,
            backtrace: std::backtrace::Backtrace::capture(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Clone, PartialEq, Eq, Debug)]
#[error("Dataset schema is not yet available: {dataset_ref}")]
pub struct DatasetSchemaNotAvailableError {
    pub dataset_ref: odf::DatasetRef,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<odf::DatasetRefUnresolvedError> for QueryError {
    fn from(v: odf::DatasetRefUnresolvedError) -> Self {
        match v {
            odf::DatasetRefUnresolvedError::NotFound(e) => Self::DatasetNotFound(e),
            odf::DatasetRefUnresolvedError::Internal(e) => Self::Internal(e),
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
