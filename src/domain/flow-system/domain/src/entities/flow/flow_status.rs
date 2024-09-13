// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::{self, Display, Formatter};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "flow_status_type", rename_all = "snake_case")]
pub enum FlowStatus {
    Waiting,
    Running,
    Finished,
}

impl Display for FlowStatus {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            FlowStatus::Waiting => write!(f, "waiting"),
            FlowStatus::Running => write!(f, "running"),
            FlowStatus::Finished => write!(f, "finished"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
