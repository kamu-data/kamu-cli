// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRegistry;
use kamu_flow_system::{self as fs};

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Union)]
pub(crate) enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    DatasetUpdate(FlowActivationCauseDatasetUpdate),
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
            fs::FlowActivationCause::DatasetUpdate(update) => {
                let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);

                let hdl = dataset_registry
                    .resolve_dataset_handle_by_ref(&update.dataset_id.as_local_ref())
                    .await
                    .int_err()?;
                let account = Account::from_dataset_alias(ctx, &hdl.alias)
                    .await?
                    .expect("Account must exist");
                Self::DatasetUpdate(FlowActivationCauseDatasetUpdate {
                    dataset: Dataset::new_access_checked(account, hdl),
                    source: match update.source {
                        fs::DatasetUpdateSource::UpstreamFlow { .. } => {
                            FlowActivationCauseDatasetUpdateSource::UpstreamFlow
                        }
                        fs::DatasetUpdateSource::HttpIngest { .. } => {
                            FlowActivationCauseDatasetUpdateSource::HttpIngest
                        }
                        fs::DatasetUpdateSource::SmartProtocolPush { .. } => {
                            FlowActivationCauseDatasetUpdateSource::SmartProtocolPush
                        }
                    },
                })
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
pub(crate) struct FlowActivationCauseDatasetUpdate {
    dataset: Dataset,
    source: FlowActivationCauseDatasetUpdateSource,
}

#[derive(Enum, Debug, Copy, Clone, Eq, PartialEq)]
pub(crate) enum FlowActivationCauseDatasetUpdateSource {
    UpstreamFlow,
    HttpIngest,
    SmartProtocolPush,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
