// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core as domain;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DataQueryResult
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Query executed successfully
#[derive(SimpleObject)]
pub struct DataQueryResultSuccess {
    /// Schema of the result
    pub schema: DataSchema,
    /// Records of the result
    pub data: DataBatch,
    /// Input datasets of the query and the blocks they were read at, allowing
    /// the query to be reproduced
    pub datasets: Vec<DatasetState>,
    /// Maximum number of records the result was limited to
    pub limit: u64,
}

/// Query could not be executed
#[derive(SimpleObject)]
pub struct DataQueryResultError {
    /// Description of the problem
    pub error_message: String,
    /// Category of the problem
    pub error_kind: DataQueryResultErrorKind,
}

/// Category of a query error
#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataQueryResultErrorKind {
    /// Query is malformed or refers to a dataset or block that does not exist
    InvalidSql,
}

/// Outcome of a data query
#[derive(Union)]
pub enum DataQueryResult {
    Success(DataQueryResultSuccess),
    Error(DataQueryResultError),
}

impl DataQueryResult {
    pub fn from_query_error(err: domain::QueryError) -> Result<Self, GqlError> {
        tracing::debug!(?err, "Query error");

        match err {
            domain::QueryError::DatasetNotFound(e) => Ok(Self::invalid_sql(e.to_string())),
            domain::QueryError::DatasetBlockNotFound(e) => Ok(Self::invalid_sql(e.to_string())),
            domain::QueryError::BadQuery(e) => Ok(Self::invalid_sql(e.to_string())),
            domain::QueryError::Access(e) => Err(e.into()),
            domain::QueryError::Internal(e) => Err(e.into()),
        }
    }

    pub fn success(
        schema: DataSchema,
        data: DataBatch,
        datasets: Vec<DatasetState>,
        limit: u64,
    ) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema,
            data,
            datasets,
            limit,
        })
    }

    pub fn invalid_sql(error_message: String) -> DataQueryResult {
        DataQueryResult::Error(DataQueryResultError {
            error_message,
            error_kind: DataQueryResultErrorKind::InvalidSql,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
