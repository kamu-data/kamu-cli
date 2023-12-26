// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use kamu::EngineProvisionerNull;
use kamu_core::engine::*;
use kamu_core::*;

/////////////////////////////////////////////////////////////////////////////////////////

mockall::mock! {
    pub EngineProvisioner {}
    #[async_trait::async_trait]
    impl EngineProvisioner for EngineProvisioner {
        async fn provision_engine<'a>(
            &'a self,
            engine_id: &str,
            maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
        ) -> Result<EngineHandle<'a>, EngineProvisioningError>;

        /// Do not use directly - called automatically by [EngineHandle]
        fn release_engine(&self, engine: &dyn Engine);

        /// TODO: Will be removed
        async fn provision_ingest_engine<'a>(
            &'a self,
            maybe_listener: Option<Arc<dyn EngineProvisioningListener>>,
        ) -> Result<IngestEngineHandle<'a>, EngineProvisioningError>;

        /// Do not use directly - called automatically by [IngestEngineHandle]
        fn release_ingest_engine(&self, engine: &dyn IngestEngine);
    }
}

impl MockEngineProvisioner {
    pub fn stub_provision_engine(mut self) -> Self {
        self.expect_provision_engine().return_once(|_, _| {
            Ok(EngineHandle::new(
                &EngineProvisionerNull,
                Arc::new(EngineStub {}),
            ))
        });
        self
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

pub struct EngineStub {}

#[async_trait::async_trait]
impl Engine for EngineStub {
    async fn transform(
        &self,
        _request: TransformRequest,
    ) -> Result<TransformResponse, EngineError> {
        // Note: At least 1 output field must be present, watermark is easy to mimic
        Ok(TransformResponse {
            new_offset_interval: None,
            new_watermark: Some(Utc::now()),
            new_checkpoint: None,
            new_data: None,
        })
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
