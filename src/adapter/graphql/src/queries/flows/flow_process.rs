// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use kamu_flow_system::{self as fs};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowProcess<'a> {
    process_state: Cow<'a, fs::FlowProcessState>,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> FlowProcess<'a> {
    #[graphql(skip)]
    pub fn new(process_state: Cow<'a, fs::FlowProcessState>) -> Self {
        Self { process_state }
    }

    async fn flow_type(&self) -> DatasetFlowType {
        decode_dataset_flow_type(&self.process_state.flow_binding().flow_type)
    }

    async fn summary(&self) -> FlowProcessSummary {
        (*self.process_state).clone().into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
