// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_utils::BackgroundAgent;
use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_CONTROLLER_RUNTIME_AGENT_NAME: &str =
    "dev.kamu.domain.resources.ResourceControllerRuntime";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The background agent driving every registered `ResourceController`, with a
/// loop of its own per controller
#[async_trait::async_trait]
pub trait ResourceControllerRuntime: BackgroundAgent {
    /// Processes every resource that needs attention now, controller by
    /// controller, until none is left. Does not wait for deadlines: a resource
    /// waiting for time stays for a later run.
    async fn run_until_idle(&self) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
