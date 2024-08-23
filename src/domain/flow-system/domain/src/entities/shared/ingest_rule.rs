// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

use super::Schedule;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestRule {
    pub fetch_uncacheable: bool,
    // ToDo: Schedule should be on higher level and not mixed up
    // with general configuration rules
    pub schedule_condition: Option<Schedule>,
}
