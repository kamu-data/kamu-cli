// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::DatasetRegistry;
use kamu_flow_system as fs;

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
    let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

    match flow_query_service.get_flow(flow_id.into()).await {
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
        Err(e) => {
            return match e {
                fs::GetFlowError::NotFound(_) => {
                    Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })))
                }
                fs::GetFlowError::Internal(e) => Err(GqlError::Internal(e)),
            }
        }
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_expected_dataset_kind(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
    dataset_flow_type: DatasetFlowType,
    flow_run_configuration_maybe: Option<&FlowRunConfiguration>,
) -> Result<Option<FlowIncompatibleDatasetKind>> {
    if let Some(FlowRunConfiguration::Compaction(CompactionConditionInput::MetadataOnly(_))) =
        flow_run_configuration_maybe
    {
        return Ok(None);
    }
    let dataset_flow_type: kamu_flow_system::DatasetFlowType = dataset_flow_type.into();
    match dataset_flow_type.dataset_kind_restriction() {
        Some(expected_kind) => {
            let resolved_dataset = utils::get_dataset(ctx, dataset_handle)?;

            let dataset_kind = resolved_dataset
                .get_summary(odf::dataset::GetSummaryOpts::default())
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

// Check flow preconditions and set default configurations if needed
pub(crate) async fn ensure_flow_preconditions(
    ctx: &Context<'_>,
    dataset_handle: &odf::DatasetHandle,
    dataset_flow_type: DatasetFlowType,
    flow_run_configuration: Option<&FlowRunConfiguration>,
) -> Result<Option<FlowPreconditionsNotMet>> {
    let dataset_registry = from_catalog_n!(ctx, dyn DatasetRegistry);
    let target = dataset_registry.get_dataset_by_handle(dataset_handle);

    match dataset_flow_type {
        DatasetFlowType::Ingest => {
            let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);
            let source_res = metadata_query_service
                .get_active_polling_source(target)
                .await
                .int_err()?;
            if source_res.is_none() {
                return Ok(Some(FlowPreconditionsNotMet {
                    preconditions: "No SetPollingSource event defined".to_string(),
                }));
            }
        }
        DatasetFlowType::ExecuteTransform => {
            let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);
            let source_res = metadata_query_service.get_active_transform(target).await?;
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
                use odf::dataset::MetadataChainExt as _;
                if let Some(new_head_hash) = &reset_configuration.new_head_hash() {
                    let metadata_chain = target.as_metadata_chain();
                    let current_head_hash_maybe = metadata_chain
                        .try_get_ref(&odf::BlockRef::Head)
                        .await
                        .int_err()?;
                    if current_head_hash_maybe.is_none() {
                        return Ok(Some(FlowPreconditionsNotMet {
                            preconditions: "Dataset does not contain any blocks".to_string(),
                        }));
                    }
                    if !metadata_chain
                        .contains_block(new_head_hash)
                        .await
                        .int_err()?
                    {
                        return Ok(Some(FlowPreconditionsNotMet {
                            preconditions: "New head hash not found".to_string(),
                        }));
                    };

                    if new_head_hash == &current_head_hash_maybe.unwrap().into() {
                        return Ok(Some(FlowPreconditionsNotMet {
                            preconditions: "Provided head hash is already a head block".to_string(),
                        }));
                    }
                }
            }
        }
    }
    Ok(None)
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
