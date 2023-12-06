// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{FlowStrategy, SystemFlowID, SystemFlowKey};

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone)]
pub struct SystemFlowStrategy {}

impl FlowStrategy for SystemFlowStrategy {
    type FlowID = SystemFlowID;
    type FlowKey = SystemFlowKey;
}

/////////////////////////////////////////////////////////////////////////////////////////
