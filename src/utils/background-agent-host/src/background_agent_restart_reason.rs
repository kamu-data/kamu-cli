// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Why the host restarted an agent; the `reason` label of its metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr, strum::EnumIter)]
#[strum(serialize_all = "snake_case")]
pub enum BackgroundAgentRestartReason {
    /// `run` returned an error
    Failed,
    /// `run` panicked
    Panicked,
    /// `run` returned before a shutdown
    Completed,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
