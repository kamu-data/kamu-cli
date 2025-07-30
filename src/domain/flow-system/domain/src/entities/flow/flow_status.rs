// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type, strum::Display)]
#[sqlx(type_name = "flow_status_type", rename_all = "snake_case")]
pub enum FlowStatus {
    Waiting,
    Running,
    Retrying,
    Finished,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
