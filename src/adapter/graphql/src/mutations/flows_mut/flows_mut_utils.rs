// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_datasets::{DatasetAction, ResolvedDataset};
use {kamu_adapter_flow_dataset as afs, kamu_flow_system as fs};

use super::FlowNotFound;
use crate::prelude::*;
use crate::queries::DatasetRequestState;
use crate::utils;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_scheduling_permission(
    ctx: &Context<'_>,
    dataset_request_state: &DatasetRequestState,
) -> Result<()> {
    utils::check_dataset_maintain_access(ctx, dataset_request_state).await
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
    dataset_id: &odf::DatasetID,
) -> Result<Option<FlowInDatasetError>> {
    let flow_query_service = from_catalog_n!(ctx, dyn fs::FlowQueryService);

    match flow_query_service.get_flow(flow_id.into()).await {
        Ok(flow_state) => {
            if let Some(flow_dataset_id) =
                afs::FlowScopeDataset::maybe_dataset_id_in_scope(&flow_state.flow_binding.scope)
            {
                if flow_dataset_id != *dataset_id {
                    return Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })));
                }
            } else {
                // If the flow is not bound to a dataset, treat as not found in this dataset
                return Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })));
            }
        }
        Err(e) => {
            return match e {
                fs::GetFlowError::NotFound(_) => {
                    Ok(Some(FlowInDatasetError::NotFound(FlowNotFound { flow_id })))
                }
                fs::GetFlowError::Internal(e) => Err(GqlError::Internal(e)),
            };
        }
    }

    Ok(None)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn ensure_flow_applied_to_expected_dataset_kind(
    resolved_dataset: &ResolvedDataset,
    expected_dataset_kind: odf::DatasetKind,
) -> std::result::Result<(), FlowIncompatibleDatasetKind> {
    let actual_dataset_kind = resolved_dataset.get_kind();

    if actual_dataset_kind != expected_dataset_kind {
        Err(FlowIncompatibleDatasetKind {
            expected_dataset_kind: expected_dataset_kind.into(),
            actual_dataset_kind: actual_dataset_kind.into(),
        })
    } else {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn ensure_expected_dataset_kind(
    ctx: &Context<'_>,
    dataset_request_state: &DatasetRequestState,
    dataset_flow_type: DatasetFlowType,
) -> Result<Option<FlowIncompatibleDatasetKind>> {
    let maybe_expected_kind = match dataset_flow_type {
        DatasetFlowType::Ingest | DatasetFlowType::HardCompaction => Some(odf::DatasetKind::Root),
        DatasetFlowType::ExecuteTransform => Some(odf::DatasetKind::Derivative),
        DatasetFlowType::Reset | DatasetFlowType::ResetToMetadata => None,
    };

    match maybe_expected_kind {
        Some(expected_kind) => {
            let resolved_dataset = dataset_request_state.resolved_dataset(ctx).await?;
            let dataset_kind = resolved_dataset.get_kind();

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
    dataset_request_state: &DatasetRequestState,
    dataset_flow_type: DatasetFlowType,
    reset_configuration: Option<&FlowConfigResetInput>,
) -> Result<Option<FlowPreconditionsNotMet>> {
    let resolved_dataset = dataset_request_state.resolved_dataset(ctx).await?;

    match dataset_flow_type {
        DatasetFlowType::Ingest => {
            let metadata_query_service = from_catalog_n!(ctx, dyn kamu_core::MetadataQueryService);
            let source_res = metadata_query_service
                .get_active_polling_source(resolved_dataset)
                .await
                .int_err()?;
            if source_res.is_none() {
                return Ok(Some(FlowPreconditionsNotMet {
                    preconditions: "No SetPollingSource event defined".to_string(),
                }));
            }
        }

        DatasetFlowType::ExecuteTransform => {
            let (metadata_query_service, rebac_dataset_registry_facade) = from_catalog_n!(
                ctx,
                dyn kamu_core::MetadataQueryService,
                dyn RebacDatasetRegistryFacade
            );
            let source_res = metadata_query_service
                .get_active_transform(resolved_dataset)
                .await?;

            match source_res {
                Some((_, set_transform_block)) => {
                    let set_transform = set_transform_block.event;
                    let inputs_dataset_refs = set_transform
                        .inputs
                        .iter()
                        .map(|input| &input.dataset_ref)
                        .collect::<Vec<_>>();

                    let classify_response = rebac_dataset_registry_facade
                        .classify_dataset_refs_by_allowance(
                            &inputs_dataset_refs,
                            DatasetAction::Read,
                        )
                        .await?;

                    if !classify_response.inaccessible_refs.is_empty() {
                        let dataset_ref_alias_map = set_transform.as_dataset_ref_alias_map();

                        return Ok(Some(FlowPreconditionsNotMet {
                            preconditions: classify_response
                                .into_inaccessible_input_datasets_message(&dataset_ref_alias_map),
                        }));
                    }
                }
                None => {
                    return Ok(Some(FlowPreconditionsNotMet {
                        preconditions: "No SetTransform event defined".to_string(),
                    }));
                }
            }
        }

        DatasetFlowType::HardCompaction | DatasetFlowType::ResetToMetadata => (),

        DatasetFlowType::Reset => {
            if let Some(reset_configuration) = reset_configuration {
                use odf::dataset::MetadataChainExt as _;
                if let Some(new_head_hash) = &reset_configuration.new_head_hash() {
                    let metadata_chain = resolved_dataset.as_metadata_chain();
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
                    }

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

#[derive(SimpleObject, Debug)]
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

#[derive(SimpleObject, Debug)]
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
