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

use crate::ResourceControllerDrainSession;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const RESOURCE_CONTROLLER_RUNTIME_AGENT_NAME: &str =
    "dev.kamu.domain.resources.ResourceControllerRuntime";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The background agent driving every registered `ResourceController`, with a
/// loop of its own per controller
pub trait ResourceControllerRuntime: BackgroundAgent {
    /// Drains on demand, for a caller that does not run the agent, such as a
    /// CLI command processing what it changed before it exits
    fn new_drain_session(&self) -> Result<Box<dyn ResourceControllerDrainSession>, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
