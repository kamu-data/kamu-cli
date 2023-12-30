// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use container_runtime::{ImagePullError, PullImageListener};
use internal_error::InternalError;
use thiserror::Error;

use crate::entities::engine::Engine;

///////////////////////////////////////////////////////////////////////////////
// EngineProvisioner
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait EngineProvisioner: Send + Sync {
    async fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<Arc<dyn Engine>, EngineProvisioningError>;
}

///////////////////////////////////////////////////////////////////////////////
// Listener
///////////////////////////////////////////////////////////////////////////////

pub trait EngineProvisioningListener: Send + Sync {
    fn begin(&self, _engine_id: &str) {}
    fn success(&self) {}

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        None
    }
}

pub struct NullEngineProvisioningListener;
impl EngineProvisioningListener for NullEngineProvisioningListener {}

///////////////////////////////////////////////////////////////////////////////
// Errors
///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum EngineProvisioningError {
    #[error(transparent)]
    ImagePull(#[from] ImagePullError),
    #[error(transparent)]
    Internal(#[from] InternalError),
}
