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

#[derive(SimpleObject)]
pub struct DataQueryResultSuccess {
    pub schema: Option<DataSchema>,
    pub data: DataBatch,
    pub datasets: Option<Vec<DatasetState>>,
    pub limit: u64,
}

#[derive(SimpleObject)]
pub struct DataQueryResultError {
    pub error_message: String,
    pub error_kind: DataQueryResultErrorKind,
}

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataQueryResultErrorKind {
    InvalidSql,
}

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
            domain::QueryError::DatasetSchemaNotAvailable(_) => {
                // NOTE: This error must be handled on upper level to return empty collection
                unreachable!()
            }
            domain::QueryError::Access(e) => Err(e.into()),
            domain::QueryError::Internal(e) => Err(e.into()),
        }
    }

    pub fn success(
        schema: Option<DataSchema>,
        data: DataBatch,
        datasets: Option<Vec<DatasetState>>,
        limit: u64,
    ) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema,
            data,
            datasets,
            limit,
        })
    }

    pub fn no_schema_yet(format: DataBatchFormat, limit: u64) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema: None,
            data: DataBatch::empty(format),
            datasets: None,
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
