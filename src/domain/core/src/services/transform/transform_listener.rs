// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{TransformElaborateError, TransformExecuteError, TransformResult};
use crate::EngineProvisioningListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait TransformListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _result: &TransformResult) {}
    fn elaborate_error(&self, _error: &TransformElaborateError) {}
    fn execute_error(&self, _error: &TransformExecuteError) {}

    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        None
    }
}

pub struct NullTransformListener;
impl TransformListener for NullTransformListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait TransformMultiListener: Send + Sync {
    fn begin_transform(&self, _dataset: &odf::DatasetHandle) -> Option<Arc<dyn TransformListener>> {
        None
    }
}

pub struct NullTransformMultiListener;
impl TransformMultiListener for NullTransformMultiListener {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
