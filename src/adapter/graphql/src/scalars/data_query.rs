// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::error::DataFusionError;
use kamu_core::QueryError;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////
// DataQueryResult
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
pub struct DataQueryResultSuccess {
    pub schema: Option<DataSchema>,
    pub data: DataBatch,
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
    InternalError,
}

#[derive(Union)]
pub enum DataQueryResult {
    Success(DataQueryResultSuccess),
    Error(DataQueryResultError),
}

impl DataQueryResult {
    pub fn success(schema: Option<DataSchema>, data: DataBatch, limit: u64) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema,
            data,
            limit,
        })
    }

    pub fn no_schema_yet(format: DataBatchFormat, limit: u64) -> DataQueryResult {
        DataQueryResult::Success(DataQueryResultSuccess {
            schema: None,
            data: DataBatch::empty(format),
            limit,
        })
    }

    pub fn invalid_sql(error_message: String) -> DataQueryResult {
        DataQueryResult::Error(DataQueryResultError {
            error_message,
            error_kind: DataQueryResultErrorKind::InvalidSql,
        })
    }

    pub fn internal(error_message: String) -> DataQueryResult {
        DataQueryResult::Error(DataQueryResultError {
            error_message,
            error_kind: DataQueryResultErrorKind::InternalError,
        })
    }
}

impl From<QueryError> for DataQueryResult {
    fn from(e: QueryError) -> Self {
        match e {
            QueryError::DatasetNotFound(e) => DataQueryResult::invalid_sql(e.to_string()),
            QueryError::DataFusionError(e) => e.into(),
            QueryError::DatasetSchemaNotAvailable(_) => unreachable!(),
            QueryError::Internal(e) => DataQueryResult::internal(e.to_string()),
        }
    }
}

impl From<DataFusionError> for DataQueryResult {
    fn from(e: DataFusionError) -> Self {
        match e {
            DataFusionError::SQL(e) => DataQueryResult::invalid_sql(e.to_string()),
            DataFusionError::Plan(e) => DataQueryResult::invalid_sql(e),
            _ => DataQueryResult::internal(e.to_string()),
        }
    }
}
