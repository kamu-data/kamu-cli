// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_flow_system::{FlowBinding, FlowConfigurationService};

use crate::prelude::*;
use crate::queries::DatasetRequestState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetFlowConfigs<'a> {
    dataset_request_state: &'a DatasetRequestState,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> DatasetFlowConfigs<'a> {
    #[graphql(skip)]
    pub fn new(dataset_request_state: &'a DatasetRequestState) -> Self {
        Self {
            dataset_request_state,
        }
    }

    /// Returns defined configuration for a flow of specified type
    #[tracing::instrument(level = "info", name = DatasetFlowConfigs_by_type, skip_all, fields(?dataset_flow_type))]
    async fn by_type(
        &self,
        ctx: &Context<'_>,
        dataset_flow_type: DatasetFlowType,
    ) -> Result<Option<FlowConfiguration>> {
        let flow_binding = FlowBinding::for_dataset(
            self.dataset_request_state.dataset_handle().id.clone(),
            map_dataset_flow_type(dataset_flow_type),
        );

        let flow_config_service = from_catalog_n!(ctx, dyn FlowConfigurationService);
        let maybe_flow_config = flow_config_service
            .find_configuration(&flow_binding)
            .await
            .int_err()?;

        Ok(maybe_flow_config.map(Into::into))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
