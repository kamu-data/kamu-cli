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
use kamu_datasets::ResolvedDatasetsMap;

use crate::engine::*;
use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl MockEngineProvisioner {
    pub fn stub_provision_engine(mut self) -> Self {
        self.expect_provision_engine()
            .return_once(|_, _| Ok(Arc::new(EngineStub::advancing_watermark())));
        self
    }

    pub fn always_provision_engine(mut self) -> Self {
        self.expect_provision_engine()
            .returning(|_, _| Ok(Arc::new(EngineStub::advancing_watermark())));
        self
    }

    /// Provisions engines whose every transform returns `make_response()`
    pub fn always_provision_engine_responding(
        mut self,
        make_response: impl Fn() -> TransformResponseExt + Send + Sync + 'static,
    ) -> Self {
        let make_response: Arc<dyn Fn() -> TransformResponseExt + Send + Sync> =
            Arc::new(make_response);
        self.expect_provision_engine().returning(move |_, _| {
            Ok(Arc::new(EngineStub {
                make_response: make_response.clone(),
            }))
        });
        self
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EngineStub {
    make_response: Arc<dyn Fn() -> TransformResponseExt + Send + Sync>,
}

impl EngineStub {
    // Note: At least 1 output field must be present, watermark is easy to mimic
    fn advancing_watermark() -> Self {
        Self {
            make_response: Arc::new(|| TransformResponseExt {
                new_offset_interval: None,
                new_watermark: Some(Utc::now()),
                output_schema: None,
                new_checkpoint: None,
                new_data: None,
                run_dir: None,
            }),
        }
    }
}

#[async_trait::async_trait]
impl Engine for EngineStub {
    async fn execute_raw_query(
        &self,
        _request: RawQueryRequestExt,
    ) -> Result<RawQueryResponseExt, EngineError> {
        unimplemented!()
    }

    async fn execute_transform(
        &self,
        _request: TransformRequestExt,
        _operation_id: &str,
        _datasets_map: &ResolvedDatasetsMap,
    ) -> Result<TransformResponseExt, EngineError> {
        Ok((self.make_response)())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
