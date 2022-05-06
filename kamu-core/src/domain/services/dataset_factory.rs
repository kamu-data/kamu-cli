// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::domain::*;

use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use url::Url;

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait DatasetFactory: Send + Sync {
    fn get_dataset(&self, url: Url) -> Result<Arc<dyn Dataset>, GetDatasetError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetError {
    #[error(transparent)]
    UnsupportedProtocol(#[from] UnsupportedProtocolError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct UnsupportedProtocolError {
    pub url: Url,
}

impl std::fmt::Display for UnsupportedProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "usupported protocol {} when accessing dataset at {}",
            self.url.scheme(),
            self.url
        )
    }
}
