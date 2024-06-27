// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::path::Path;

use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::*;
use internal_error::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// A common interface for readers that implement support for various formats
/// defined in the [ReadStep].
#[async_trait::async_trait]
pub trait Reader: Send + Sync {
    /// Returns schema that the input will be coerced into, if such schema
    /// is defined explicitly.
    async fn input_schema(&self) -> Option<Schema>;

    /// Returns a [DataFrame] with a logical plan set up to read the data.
    ///
    /// Note that [DataFrame] represents a logical plan, and data has not been
    /// fully read yet when function returns, so you will need to handle read
    /// errors when consuming the data. Some input data may be touched to
    /// infer the schema if one was not specified explicitly.
    async fn read(&self, path: &Path) -> Result<DataFrame, ReadError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum ReadError {
    #[error(transparent)]
    BadInput(#[from] BadInputError),
    #[error(transparent)]
    Unsupported(#[from] UnsupportedError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("{message}")]
pub struct BadInputError {
    message: String,
    backtrace: Backtrace,
}

impl BadInputError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
#[error("{message}")]
pub struct UnsupportedError {
    message: String,
    backtrace: Backtrace,
}

impl UnsupportedError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
