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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlows {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlows {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Returns interface for flow configurations queries
    async fn configs(&self) -> DatasetFlowConfigs {
        DatasetFlowConfigs::new(self.dataset_handle.clone())
    }

    /// Returns interface for flow triggers queries
    async fn triggers(&self) -> DatasetFlowTriggers {
        DatasetFlowTriggers::new(self.dataset_handle.clone())
    }

    /// Returns interface for flow runs queries
    async fn runs(&self) -> DatasetFlowRuns {
        DatasetFlowRuns::new(self.dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
