// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{self as fs};

use crate::prelude::*;
use crate::queries::{Dataset, DatasetRequestStateWithOwner};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowProcess {
    dataset_request_state: DatasetRequestStateWithOwner,
    process_state: fs::FlowProcessState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl DatasetFlowProcess {
    #[graphql(skip)]
    pub fn new(
        dataset_request_state: DatasetRequestStateWithOwner,
        process_state: fs::FlowProcessState,
    ) -> Self {
        Self {
            dataset_request_state,
            process_state,
        }
    }

    pub async fn flow_type(&self) -> DatasetFlowType {
        decode_dataset_flow_type(&self.process_state.flow_binding().flow_type)
    }

    pub async fn dataset(&self) -> Dataset {
        Dataset::new_access_checked(
            self.dataset_request_state.owner().clone(),
            self.dataset_request_state.dataset_handle().clone(),
        )
    }

    pub async fn summary(&self) -> FlowProcessSummary {
        self.process_state.clone().into()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

page_based_connection!(
    DatasetFlowProcess,
    DatasetFlowProcessConnection,
    DatasetFlowProcessEdge
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
