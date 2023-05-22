// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::sync::Arc;

use container_runtime::{ImagePullError, PullImageListener};
use thiserror::Error;

use super::engine::{Engine, IngestEngine};
use super::InternalError;

///////////////////////////////////////////////////////////////////////////////
// EngineProvisioner
///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait EngineProvisioner: Send + Sync {
    async fn provision_engine(
        &self,
        engine_id: &str,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<EngineHandle, EngineProvisioningError>;

    /// Do not use directly - called automatically by [EngineHandle]
    fn release_engine(&self, engine: &dyn Engine);

    /// TODO: Will be removed
    async fn provision_ingest_engine(
        &self,
        maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
    ) -> Result<IngestEngineHandle, EngineProvisioningError>;

    /// Do not use directly - called automatically by [IngestEngineHandle]
    fn release_ingest_engine(&self, engine: &dyn IngestEngine);
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

///////////////////////////////////////////////////////////////////////////////
// EngineHandle
///////////////////////////////////////////////////////////////////////////////

pub struct EngineHandle<'a> {
    provisioner: &'a dyn EngineProvisioner,
    engine: Arc<dyn Engine>,
}

impl<'a> EngineHandle<'a> {
    pub(crate) fn new(provisioner: &'a dyn EngineProvisioner, engine: Arc<dyn Engine>) -> Self {
        Self {
            provisioner,
            engine,
        }
    }
}

impl<'a> Deref for EngineHandle<'a> {
    type Target = dyn Engine;

    fn deref(&self) -> &Self::Target {
        self.engine.as_ref()
    }
}

impl<'a> Drop for EngineHandle<'a> {
    fn drop(&mut self) {
        self.provisioner.release_engine(self.engine.as_ref());
    }
}

///////////////////////////////////////////////////////////////////////////////

pub struct IngestEngineHandle<'a> {
    provisioner: &'a dyn EngineProvisioner,
    engine: Arc<dyn IngestEngine>,
}

impl<'a> IngestEngineHandle<'a> {
    pub(crate) fn new(
        provisioner: &'a dyn EngineProvisioner,
        engine: Arc<dyn IngestEngine>,
    ) -> Self {
        Self {
            provisioner,
            engine,
        }
    }
}

impl<'a> Deref for IngestEngineHandle<'a> {
    type Target = dyn IngestEngine;

    fn deref(&self) -> &Self::Target {
        self.engine.as_ref()
    }
}

impl<'a> Drop for IngestEngineHandle<'a> {
    fn drop(&mut self) {
        self.provisioner.release_ingest_engine(self.engine.as_ref());
    }
}
