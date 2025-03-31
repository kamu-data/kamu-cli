// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use internal_error::{BoxedError, InternalError};
use odf::utils::data::DataFrameExt;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Service
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ExportService: Send + Sync {
    async fn export_to_fs(
        &self,
        df: DataFrameExt,
        path: &Path,
        options: ExportOptions,
    ) -> Result<u64, ExportError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, strum::Display, Debug, strum::EnumIter)]
pub enum ExportFormat {
    #[strum(to_string = "parquet")]
    Parquet,

    #[strum(to_string = "csv")]
    Csv,

    #[strum(to_string = "ndjson")]
    NdJson,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ExportOptions {
    pub format: ExportFormat,
    pub records_per_file: Option<usize>,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ExportFormat::Parquet,
            records_per_file: None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
