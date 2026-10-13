// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_adapter_flow_dataset::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
};
use kamu_flow_system::{self as fs};

use crate::prelude::*;
use crate::queries::{Account, Dataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reason a flow was started
#[derive(Union)]
pub(crate) enum FlowActivationCause {
    Manual(FlowActivationCauseManual),
    AutoPolling(FlowActivationCauseAutoPolling),
    DatasetUpdate(FlowActivationCauseDatasetUpdate),
    IterationFinished(FlowActivationCauseIterationFinished),
}

impl FlowActivationCause {
    pub async fn build(
        activation_cause: &fs::FlowActivationCause,
        ctx: &Context<'_>,
    ) -> Result<Self> {
        Ok(match activation_cause {
            fs::FlowActivationCause::Manual(manual) => {
                let initiator =
                    Account::from_account_id(ctx, manual.initiator_account_id.clone()).await?;
                Self::Manual(FlowActivationCauseManual { initiator })
            }
            fs::FlowActivationCause::AutoPolling(auto_polling) => {
                Self::AutoPolling(auto_polling.clone().into())
            }
            fs::FlowActivationCause::IterationFinished(iteration_finished) => {
                Self::IterationFinished(iteration_finished.clone().into())
            }
            fs::FlowActivationCause::ResourceUpdate(update) => {
                if update.resource_type != DATASET_RESOURCE_TYPE {
                    return Err(GqlError::Internal(InternalError::new(format!(
                        "Unexpected resource type in flow activation cause: {}",
                        update.resource_type
                    ))));
                }

                let update_dataset_details: DatasetResourceUpdateDetails =
                    serde_json::from_value(update.details.clone()).int_err()?;

                let dataset_id = update_dataset_details.dataset_id;
                let dataset = Dataset::try_from_ref(ctx, &dataset_id.as_local_ref()).await?;

                Self::DatasetUpdate(FlowActivationCauseDatasetUpdate {
                    dataset_id: dataset_id.into(),
                    dataset,
                    source: match update_dataset_details.source {
                        DatasetUpdateSource::UpstreamFlow { flow_id, .. } => {
                            FlowActivationCauseDatasetUpdateSource::UpstreamFlow(
                                FlowActivationCauseDatasetUpdateSourceUpstreamFlow {
                                    flow_id: FlowID::from(flow_id),
                                },
                            )
                        }
                        DatasetUpdateSource::HttpIngest { source_name } => {
                            FlowActivationCauseDatasetUpdateSource::HttpIngest(
                                FlowActivationCauseDatasetUpdateSourceHttpIngest { source_name },
                            )
                        }
                        DatasetUpdateSource::SmartProtocolPush {
                            account_name,
                            is_force,
                        } => FlowActivationCauseDatasetUpdateSource::SmartProtocolPush(
                            FlowActivationCauseDatasetUpdateSourceSmartProtocolPush {
                                is_force,
                                account_name: account_name.map(Into::into),
                            },
                        ),
                        DatasetUpdateSource::ExternallyDetectedChange => {
                            FlowActivationCauseDatasetUpdateSource::ExternallyDetectedChange(
                                FlowActivationCauseDatasetUpdateSourceExternallyDetectedChange {
                                    dummy: true,
                                },
                            )
                        }
                    },
                })
            }
        })
    }
}

/// Flow was started manually by a user
#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseManual {
    /// Account that started the flow
    initiator: Account,
}

/// Flow was started by a schedule or after a restart
#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseAutoPolling {
    /// Placeholder without meaning, always true
    dummy: bool,
}

impl From<fs::FlowActivationCauseAutoPolling> for FlowActivationCauseAutoPolling {
    fn from(_: fs::FlowActivationCauseAutoPolling) -> Self {
        Self { dummy: true }
    }
}

/// Flow was started because the previous ingest left more data to fetch
#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseIterationFinished {
    /// Placeholder without meaning, always true
    dummy: bool,
}

impl From<fs::FlowActivationCauseIterationFinished> for FlowActivationCauseIterationFinished {
    fn from(_: fs::FlowActivationCauseIterationFinished) -> Self {
        Self { dummy: true }
    }
}

/// Flow was started by an update of a dataset
#[derive(SimpleObject)]
pub(crate) struct FlowActivationCauseDatasetUpdate {
    /// Identifier of the updated dataset
    dataset_id: DatasetID<'static>,
    /// Null when the dataset was deleted or is not readable by the caller
    dataset: Option<Dataset>,
    /// What updated the dataset
    source: FlowActivationCauseDatasetUpdateSource,
}

/// What updated the dataset
#[derive(Union, Debug, Clone, Eq, PartialEq)]
pub(crate) enum FlowActivationCauseDatasetUpdateSource {
    UpstreamFlow(FlowActivationCauseDatasetUpdateSourceUpstreamFlow),
    HttpIngest(FlowActivationCauseDatasetUpdateSourceHttpIngest),
    SmartProtocolPush(FlowActivationCauseDatasetUpdateSourceSmartProtocolPush),
    ExternallyDetectedChange(FlowActivationCauseDatasetUpdateSourceExternallyDetectedChange),
}

/// Dataset was updated by another flow
#[derive(SimpleObject, Debug, Clone, Eq, PartialEq)]
pub(crate) struct FlowActivationCauseDatasetUpdateSourceUpstreamFlow {
    /// Identifier of the flow that updated the dataset
    flow_id: FlowID,
}

/// Dataset was updated by data pushed over HTTP
#[derive(SimpleObject, Debug, Clone, Eq, PartialEq)]
pub(crate) struct FlowActivationCauseDatasetUpdateSourceHttpIngest {
    /// Name of the push source, null for the default source
    source_name: Option<String>,
}

/// Dataset was updated by a push over the smart transfer protocol
#[derive(SimpleObject, Debug, Clone, Eq, PartialEq)]
pub(crate) struct FlowActivationCauseDatasetUpdateSourceSmartProtocolPush {
    /// Account that pushed the data, null if unknown
    account_name: Option<AccountName<'static>>,
    /// Whether the push was forced, possibly rewriting history
    is_force: bool,
}

/// Dataset was changed outside of the flow system
#[derive(SimpleObject, Debug, Clone, Eq, PartialEq)]
pub(crate) struct FlowActivationCauseDatasetUpdateSourceExternallyDetectedChange {
    /// Placeholder without meaning, always true
    dummy: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
