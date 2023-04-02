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
    async fn get_dataset(
        &self,
        url: &Url,
        create_if_not_exists: bool,
    ) -> Result<Arc<dyn Dataset>, BuildDatasetError>;
}

/////////////////////////////////////////////////////////////////////////////////////////
// Errors
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum BuildDatasetError {
    #[error(transparent)]
    UnsupportedProtocol(#[from] UnsupportedProtocolError),
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub struct UnsupportedProtocolError {
    pub message: Option<String>,
    pub url: Url,
}

impl std::fmt::Display for UnsupportedProtocolError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(msg) = &self.message {
            write!(f, "{}", msg)
        } else {
            write!(
                f,
                "Usupported protocol {} when accessing dataset at {}",
                self.url.scheme(),
                self.url
            )
        }
    }
}
