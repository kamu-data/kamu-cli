// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::TransformResult;
use crate::{EngineProvisioningListener, PullError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Progress of a transform. The executor reports `begin`; the pull ends it
/// with `success` once the result is committed, or with `error` for any
/// failure of the job
pub trait TransformListener: Send + Sync {
    fn begin(&self) {}
    fn success(&self, _result: &TransformResult) {}
    fn error(&self, _error: &PullError) {}

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
