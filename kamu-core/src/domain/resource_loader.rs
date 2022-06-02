// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::{BoxedError, InternalError};
use opendatafabric::DatasetSnapshot;

use std::backtrace::Backtrace;
use thiserror::Error;

pub trait ResourceLoader: Send + Sync {
    fn load_dataset_snapshot_from_path(
        &self,
        path: &std::path::Path,
    ) -> Result<DatasetSnapshot, ResourceError>;

    fn load_dataset_snapshot_from_url(
        &self,
        url: &url::Url,
    ) -> Result<DatasetSnapshot, ResourceError>;

    fn load_dataset_snapshot_from_ref(&self, sref: &str) -> Result<DatasetSnapshot, ResourceError>;
}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ResourceError {
    #[error("Source is unreachable at {path}")]
    Unreachable {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },
    #[error("Source not found at {path}")]
    NotFound {
        path: String,
        #[source]
        source: Option<BoxedError>,
    },
    #[error("Could not deserialize data")]
    SerdeError {
        #[source]
        source: BoxedError,
        backtrace: Backtrace,
    },
    #[error(transparent)]
    InternalError(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl ResourceError {
    pub fn unreachable(path: String, source: Option<BoxedError>) -> Self {
        Self::Unreachable {
            path: path,
            source: source,
        }
    }

    pub fn not_found(path: String, source: Option<BoxedError>) -> Self {
        Self::NotFound {
            path: path,
            source: source,
        }
    }

    pub fn serde(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::SerdeError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
}
