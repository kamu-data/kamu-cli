// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowConfigurationService, FlowKeyDataset};

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigs {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetFlowConfigs {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Returns defined configuration for a flow of specified type
    async fn by_type(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<Option<FlowConfiguration>> {
        let flow_config_service = from_catalog_n!(ctx, dyn FlowConfigurationService);
        let maybe_flow_config = flow_config_service
            .find_configuration(
                FlowKeyDataset::new(self.dataset_handle.id.clone(), dataset_flow_type.into())
                    .into(),
            )
            .await
            .int_err()?;

        Ok(maybe_flow_config.map(Into::into))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
