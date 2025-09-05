// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowProcessGroupRollup {
    pub active: u32,
    pub failing: u32,
    pub paused: u32,
    pub stopped: u32,
    pub total: u32,
    pub worst_consecutive_failures: u32,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(feature = "sqlx")]
#[derive(Debug, sqlx::FromRow)]
pub struct FlowProcessGroupRollupRowModel {
    total: i64,
    active: i64,
    failing: i64,
    paused: i64,
    stopped: i64,
    worst: i64,
}

#[cfg(feature = "sqlx")]
impl TryFrom<FlowProcessGroupRollupRowModel> for FlowProcessGroupRollup {
    type Error = InternalError;

    fn try_from(r: FlowProcessGroupRollupRowModel) -> Result<Self, Self::Error> {
        // Helper to keep the conversion readable and uniform
        fn c(x: i64) -> Result<u32, InternalError> {
            u32::try_from(x).int_err()
        }

        Ok(Self {
            total: c(r.total)?,
            active: c(r.active)?,
            failing: c(r.failing)?,
            paused: c(r.paused)?,
            stopped: c(r.stopped)?,
            worst_consecutive_failures: c(r.worst)?,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
