// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{DatasetFlowConfigsMut, DatasetFlowRunsMut, DatasetFlowTriggersMut};
use crate::mutations::ensure_scheduling_permission;
use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowsMut<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[Object]
impl<'a> DatasetFlowsMut<'a> {
    #[graphql(skip)]
    pub async fn new(
        ctx: &Context<'_>,
        dataset_request_state: &'a DatasetRequestState,
    ) -> Result<Self> {
        ensure_scheduling_permission(ctx, dataset_request_state).await?;

        Ok(Self {
            dataset_request_state,
        })
    }

    async fn configs(&self) -> DatasetFlowConfigsMut {
        DatasetFlowConfigsMut::new(self.dataset_request_state)
    }

    async fn runs(&self) -> DatasetFlowRunsMut {
        DatasetFlowRunsMut::new(self.dataset_request_state)
    }

    async fn triggers(&self) -> DatasetFlowTriggersMut {
        DatasetFlowTriggersMut::new(self.dataset_request_state)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
