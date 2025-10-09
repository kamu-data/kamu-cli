// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use strum::{Display, EnumCount, EnumString};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Represents the reason why a flow process was automatically stopped
#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    EnumCount,
    Display,
    EnumString,
    serde::Serialize,
    serde::Deserialize,
)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "flow_process_auto_stop_reason", rename_all = "snake_case")
)]
#[strum(serialize_all = "snake_case")]
pub enum FlowProcessAutoStopReason {
    /// Flow was auto-stopped due to stop policy (e.g., consecutive failures
    /// threshold)
    StopPolicy,
    /// Flow was auto-stopped due to an unrecoverable failure
    UnrecoverableFailure,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
