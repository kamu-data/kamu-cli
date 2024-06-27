// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system as fs;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject)]
#[graphql(complex)]
pub(crate) struct FlowNotFound {
    pub flow_id: FlowID,
}

#[ComplexObject]
impl FlowNotFound {
    pub async fn message(&self) -> String {
        let flow_id: fs::FlowID = self.flow_id.into();
        format!("Flow '{flow_id}' was not found")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
