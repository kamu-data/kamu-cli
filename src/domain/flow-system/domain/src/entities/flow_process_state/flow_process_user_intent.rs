// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(
    Debug,
    Clone,
    Copy,
    Eq,
    PartialEq,
    Hash,
    Ord,
    PartialOrd,
    strum::EnumCount,
    strum::Display,
    strum::EnumString,
    serde::Serialize,
    serde::Deserialize,
)]
#[cfg_attr(
    feature = "sqlx",
    derive(sqlx::Type),
    sqlx(type_name = "flow_process_user_intent", rename_all = "snake_case")
)]
#[strum(serialize_all = "snake_case")]
pub enum FlowProcessUserIntent {
    /// The user never enabled or disabled the flow process yet
    Undefined,

    /// The flow is intended to be running, unless auto-stopped
    Enabled,

    /// The flow is intended to be paused manually
    Paused,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
