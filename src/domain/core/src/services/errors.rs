// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;

use thiserror::Error;

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
