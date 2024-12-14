// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{Display, Formatter};

use internal_error::{BoxedError, InternalError};
use thiserror::Error;
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ExportService: Send + Sync {
    async fn export_to_fs(
        &self,
        sql_query: &str,
        path: &str,
        format: ExportFormat,
        partition_row_count: Option<usize>,
    ) -> Result<u64, ExportError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ExportFormat {
    Parquet,
    Csv,
    NdJson,
}

impl Display for ExportFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_val = match *self {
            ExportFormat::Parquet => "parquet",
            ExportFormat::Csv => "csv",
            ExportFormat::NdJson => "ndjson",
        };
        f.write_str(str_val)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("{context}")]
pub struct NotImplementedError {
    pub context: String,
}

#[derive(Debug, Error)]
#[error("Invalid SQL query: {context}")]
pub struct ExportQueryError {
    pub context: String,
    #[source]
    pub source: BoxedError,
}

#[derive(Debug, Error)]
pub enum ExportError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
    #[error(transparent)]
    NotImplemented(
        #[from]
        #[backtrace]
        NotImplementedError,
    ),
    #[error(transparent)]
    Query(ExportQueryError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
