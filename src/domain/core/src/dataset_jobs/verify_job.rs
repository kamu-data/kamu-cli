// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use crate::{VerificationListener, VerificationRequest, VerificationResult};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Verifies one dataset. In a short transaction it authorizes reading the
/// dataset and the inputs a replay reads, and plans; it then checks integrity
/// and replays transforms with no transaction open. `kamu verify`,
/// `kamu system diagnose` and the verification before `kamu system compact`
/// run it
#[async_trait::async_trait]
pub trait VerifyJob: Send + Sync {
    async fn run(
        &self,
        request: VerificationRequest<odf::DatasetHandle>,
        maybe_listener: Option<Arc<dyn VerificationListener>>,
    ) -> VerificationResult;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
