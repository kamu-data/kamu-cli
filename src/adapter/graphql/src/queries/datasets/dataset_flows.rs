// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{DatasetFlowConfigs, DatasetFlowRuns, DatasetFlowTriggers};
use crate::prelude::*;
use crate::queries::{DatasetFlowProcesses, DatasetRequestState};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlows<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[Object]
impl<'a> DatasetFlows<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Returns interface for flow configurations queries
    async fn configs(&self) -> DatasetFlowConfigs {
        DatasetFlowConfigs::new(self.dataset_request_state)
    }

    /// Returns interface for flow triggers queries
    async fn triggers(&self) -> DatasetFlowTriggers {
        DatasetFlowTriggers::new(self.dataset_request_state)
    }

    /// Returns interface for flow runs queries
    async fn runs(&self) -> DatasetFlowRuns {
        DatasetFlowRuns::new(self.dataset_request_state)
    }

    /// Returns interface for flow processes queries
    async fn processes(&self) -> DatasetFlowProcesses {
        DatasetFlowProcesses::new(self.dataset_request_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
