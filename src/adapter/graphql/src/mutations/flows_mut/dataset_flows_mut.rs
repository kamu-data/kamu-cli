// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use super::{DatasetFlowConfigsMut, DatasetFlowRunsMut, DatasetFlowTriggersMut};
use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowsMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowsMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    async fn configs(&self) -> DatasetFlowConfigsMut {
        DatasetFlowConfigsMut::new(self.dataset_handle.clone())
    }

    async fn runs(&self) -> DatasetFlowRunsMut {
        DatasetFlowRunsMut::new(self.dataset_handle.clone())
    }

    async fn triggers(&self) -> DatasetFlowTriggersMut {
        DatasetFlowTriggersMut::new(self.dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
