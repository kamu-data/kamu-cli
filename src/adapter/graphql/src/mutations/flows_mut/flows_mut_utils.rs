// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{GetBlockError, GetSummaryOpts, MetadataChainExt};
use odf::DatasetHandle;
use {kamu_flow_system as fs, opendatafabric as odf};

use super::FlowNotFound;
use crate::prelude::*;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_scheduling_permission(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
) -> Result<()> {
    utils::check_dataset_write_access(ctx, dataset_handle).await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub(crate) enum FlowInDatasetError {
    NotFound(FlowNotFound),
}

pub(crate) async fn check_if_flow_belongs_to_dataset(
    ctx: &Context<'_>,
    flow_id: FlowID,
    dataset_handle: &odf::DatasetHandle,
) -> Result<Option<FlowInDatasetError>> {
    let flow_service = from_catalog::<dyn fs::FlowService>(ctx).unwrap();

    match flow_service.get_flow(flow_id.into()).await {
        Ok(flow_state) => match flow_state.flow_key {
            fs::FlowKey::Dataset(fk_dataset) => {
                if fk_dataset.dataset_id != dataset_handle.id {
                    return Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })));
                }
            }
            fs::FlowKey::System(_) => {
                return Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })))
            }
        },
        Err(e) => match e {
            fs::GetFlowError::NotFound(_) => {
                return Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })))
            }
            fs::GetFlowError::Internal(e) => return Err(GqlError::Internal(e)),
        },
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_expected_dataset_kind(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
    dataset_flow_type: DatasetFlowType,
) -> Result<Option<FlowIncompatibleDatasetKind>> {
    let dataset_flow_type: kamu_flow_system::DatasetFlowType = dataset_flow_type.into();
    match dataset_flow_type.dataset_kind_restriction() {
        Some(expected_kind) => {
            let dataset = utils::get_dataset(ctx, dataset_handle).await?;

            let dataset_kind = dataset
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?
                .kind;

            if dataset_kind != expected_kind {
                Ok(Some(FlowIncompatibleDatasetKind {
                    expected_dataset_kind: expected_kind.into(),
                    actual_dataset_kind: dataset_kind.into(),
                }))
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_flow_preconditions(
    ctx: &Context<'_>,
    dataset_handle: &DatasetHandle,
    dataset_flow_type: DatasetFlowType,
    flow_run_configuration: &Option<FlowRunConfiguration>,
) -> Result<Option<FlowPreconditionsNotMet>> {
    match dataset_flow_type {
        DatasetFlowType::Ingest => {
            let polling_ingest_svc =
                from_catalog::<dyn kamu_core::PollingIngestService>(ctx).unwrap();
            let source_res = polling_ingest_svc
                .get_active_polling_source(&dataset_handle.as_local_ref())
                .await
                .int_err()?;
            if source_res.is_none() {
                return Ok(Some(FlowPreconditionsNotMet {
                    preconditions: "No SetPollingSource event defined".to_string(),
                }));
            }
        }
        DatasetFlowType::ExecuteTransform => {
            let transform_svc = from_catalog::<dyn kamu_core::TransformService>(ctx).unwrap();

            let source_res = transform_svc
                .get_active_transform(&dataset_handle.as_local_ref())
                .await
                .int_err()?;

            if source_res.is_none() {
                return Ok(Some(FlowPreconditionsNotMet {
                    preconditions: "No SetTransform event defined".to_string(),
                }));
            };
        }
        DatasetFlowType::HardCompaction => (),
        DatasetFlowType::Reset => {
            if let Some(flow_configuration) = flow_run_configuration
                && let FlowRunConfiguration::Reset(reset_configuration) = flow_configuration
            {
                let dataset_repo = from_catalog::<dyn kamu_core::DatasetRepository>(ctx).unwrap();

                let dataset = dataset_repo
                    .get_dataset(&dataset_handle.as_local_ref())
                    .await
                    .int_err()?;
                let new_head_block_result = dataset
                    .as_metadata_chain()
                    .get_block(&reset_configuration.new_head_hash)
                    .await;

                match new_head_block_result {
                    Err(err) => match err {
                        GetBlockError::NotFound(_) => {
                            return Ok(Some(FlowPreconditionsNotMet {
                                preconditions: "New head hash not found".to_string(),
                            }))
                        }
                        _ => return Err(err.int_err().into()),
                    },
                    Ok(existing_block) => {
                        let current_head_hash = dataset
                            .as_metadata_chain()
                            .get_block_by_ref(&kamu_core::BlockRef::Head)
                            .await
                            .int_err()?;
                        if existing_block == current_head_hash {
                            return Ok(Some(FlowPreconditionsNotMet {
                                preconditions: "Provided head hash is already a head block"
                                    .to_string(),
                            }));
                        }
                    }
                }
            }
            ()
        }
    }
    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_set_config_flow_supported(
    dataset_flow_type: DatasetFlowType,
    flow_configuration_type: &'static str,
) -> bool {
    let dataset_flow_type: kamu_flow_system::DatasetFlowType = dataset_flow_type.into();
    dataset_flow_type.config_restriction(flow_configuration_type)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub struct FlowPreconditionsNotMet {
    pub preconditions: String,
}

#[ComplexObject]
impl FlowPreconditionsNotMet {
    pub async fn message(&self) -> String {
        format!("Flow didn't met preconditions: '{}'", self.preconditions)
    }
}

#[derive(SimpleObject, Debug, Clone)]
#[graphql(complex)]
pub(crate) struct FlowIncompatibleDatasetKind {
    pub expected_dataset_kind: DatasetKind,
    pub actual_dataset_kind: DatasetKind,
}

#[ComplexObject]
impl FlowIncompatibleDatasetKind {
    pub(crate) async fn message(&self) -> String {
        format!(
            "Expected a {} dataset, but a {} dataset was provided",
            self.expected_dataset_kind, self.actual_dataset_kind,
        )
    }
}

impl std::fmt::Display for DatasetKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                DatasetKind::Root => "Root",
                DatasetKind::Derivative => "Derivative",
            }
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
