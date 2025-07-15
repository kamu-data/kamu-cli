// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
};
use kamu_core::DatasetRegistry;
use kamu_flow_system as fs;

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    Push(FlowActivationCausePush),
    InputDatasetFlow(FlowActivationCauseInputDatasetFlow),
}

impl FlowActivationCause {
    pub async fn build(
        activation_cause: &fs::FlowActivationCause,
        ctx: &Context<'_>,
    ) -> Result<Self, InternalError> {
        Ok(match &activation_cause {
            fs::FlowActivationCause::Manual(manual) => {
                let initiator =
                    Account::from_account_id(ctx, manual.initiator_account_id.clone()).await?;
                Self::Manual(FlowActivationCauseManual { initiator })
            }
            fs::FlowActivationCause::AutoPolling(auto_polling) => {
                Self::AutoPolling(auto_polling.clone().into())
            }
            fs::FlowActivationCause::Push(push) => Self::Push(push.clone().into()),
            fs::FlowActivationCause::InputDatasetFlow(input) => {
                let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

                let hdl = dataset_registry
                    .resolve_dataset_handle_by_ref(&input.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                let account = Account::from_dataset_alias(ctx, &hdl.alias)
                    .await?
                    .expect("Account must exist");
                Self::InputDatasetFlow(FlowActivationCauseInputDatasetFlow::new(
                    Dataset::new_access_checked(account, hdl),
                    match input.flow_type.as_str() {
                        FLOW_TYPE_DATASET_INGEST => DatasetFlowType::Ingest,
                        FLOW_TYPE_DATASET_RESET => DatasetFlowType::Reset,
                        FLOW_TYPE_DATASET_TRANSFORM => DatasetFlowType::ExecuteTransform,
                        FLOW_TYPE_DATASET_COMPACT => DatasetFlowType::HardCompaction,

                        _ => {
                            return InternalError::bail(format!(
                                "Unexpected flow type '{}' for input dataset flow activation cause",
                                input.flow_type.as_str()
                            ));
                        }
                    },
                    input.flow_id.into(),
                ))
            }
        })
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseManual {
    initiator: Account,
}

#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseAutoPolling {
    dummy: bool,
}

impl From<fs::FlowActivationCauseAutoPolling> for FlowActivationCauseAutoPolling {
    fn from(_: fs::FlowActivationCauseAutoPolling) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowActivationCausePush {
    dummy: bool,
}

impl From<fs::FlowActivationCausePush> for FlowActivationCausePush {
    fn from(_: fs::FlowActivationCausePush) -> Self {
        Self { dummy: true }
    }
}

#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseInputDatasetFlow {
    dataset: Dataset,
    flow_type: DatasetFlowType,
    flow_id: FlowID,
}

impl FlowActivationCauseInputDatasetFlow {
    pub fn new(dataset: Dataset, flow_type: DatasetFlowType, flow_id: FlowID) -> Self {
        Self {
            dataset,
            flow_type,
            flow_id,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
