// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::ReadService;
use crate::domain::*;

// TODO: Spark ingest is deprecated and will be removed soon leaving it only as
// an option for preprocess queries.
pub struct ReadServiceSpark {
    engine_provisioner: Arc<dyn EngineProvisioner>,
    listener: Arc<dyn IngestListener>,
}

impl ReadServiceSpark {
    pub fn new(
        engine_provisioner: Arc<dyn EngineProvisioner>,
        listener: Arc<dyn IngestListener>,
    ) -> Self {
        Self {
            engine_provisioner,
            listener,
        }
    }
}

#[async_trait::async_trait]
impl ReadService for ReadServiceSpark {
    async fn read(&self, request: IngestRequest) -> Result<IngestResponse, IngestError> {
        let engine = self
            .engine_provisioner
            .provision_ingest_engine(self.listener.clone().get_engine_provisioning_listener())
            .await?;

        let response = engine.ingest(request).await?;

        Ok(response)
    }
}
