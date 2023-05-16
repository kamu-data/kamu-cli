// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::backtrace::Backtrace;
use std::ops::Deref;
use std::sync::Arc;

use container_runtime::PullImageListener;
use thiserror::Error;

use super::engine::{Engine, IngestEngine};

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

type BoxedError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Error)]
pub enum EngineProvisioningError {
    #[error("{0}")]
    ImageNotFound(#[from] ImageNotFoundError),
    #[error("Internal error: {source}")]
    InternalError {
        source: BoxedError,
        backtrace: Backtrace,
    },
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Image not found: {image_name}")]
pub struct ImageNotFoundError {
    pub image_name: String,
    pub backtrace: Backtrace,
}

impl ImageNotFoundError {
    pub fn new(image_name: impl Into<String>) -> Self {
        Self {
            image_name: image_name.into(),
            backtrace: Backtrace::capture(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

impl EngineProvisioningError {
    pub fn image_not_found<S: Into<String>>(image_name: S) -> Self {
        Self::ImageNotFound(ImageNotFoundError::new(image_name))
    }

    pub fn internal(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::InternalError {
            source: e.into(),
            backtrace: Backtrace::capture(),
        }
    }
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
