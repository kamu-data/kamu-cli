// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{DatasetEntryServiceExt, DependencyGraphService};
use kamu_flow_system as fs;

use crate::{
    FLOW_TYPE_DATASET_COMPACT,
    FLOW_TYPE_DATASET_INGEST,
    FLOW_TYPE_DATASET_RESET,
    FLOW_TYPE_DATASET_TRANSFORM,
    FlowConfigRuleCompact,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn ingest_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_INGEST,
        fs::FlowScope::for_dataset(dataset_id.clone()),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn transform_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_TRANSFORM,
        fs::FlowScope::for_dataset(dataset_id.clone()),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn compaction_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_COMPACT,
        fs::FlowScope::for_dataset(dataset_id.clone()),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[inline]
pub fn reset_dataset_binding(dataset_id: &odf::DatasetID) -> fs::FlowBinding {
    fs::FlowBinding::new(
        FLOW_TYPE_DATASET_RESET,
        fs::FlowScope::for_dataset(dataset_id.clone()),
    )
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn trigger_metadata_only_hard_compaction_flow_for_own_downstream_datasets(
    dataset_entry_service: &dyn kamu_datasets::DatasetEntryService,
    dependency_graph_service: &dyn DependencyGraphService,
    flow_run_service: &dyn fs::FlowRunService,
    dataset_id: &odf::DatasetID,
    activation_cause: fs::FlowActivationCause,
) -> Result<(), InternalError> {
    let owner_account_id = dataset_entry_service
        .get_entry(dataset_id)
        .await
        .int_err()?
        .owner_id;

    for downstream_dataset_id in
        fetch_downstream_dataset_ids(dependency_graph_service, dataset_id).await
    {
        let owned = dataset_entry_service
            .is_dataset_owned_by(&downstream_dataset_id, &owner_account_id)
            .await
            .int_err()?;

        if owned {
            let downstream_binding = compaction_dataset_binding(&downstream_dataset_id);
            // Trigger hard compaction
            flow_run_service
                .run_flow_automatically(
                    &downstream_binding,
                    activation_cause.clone(),
                    None,
                    Some(
                        FlowConfigRuleCompact::MetadataOnly { recursive: true }.into_flow_config(),
                    ),
                    None,
                )
                .await
                .int_err()?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn fetch_downstream_dataset_ids(
    dependency_graph_service: &dyn DependencyGraphService,
    dataset_id: &odf::DatasetID,
) -> Vec<odf::DatasetID> {
    // ToDo: extend dependency graph with possibility to fetch downstream
    // dependencies by owner
    use futures::StreamExt;
    dependency_graph_service
        .get_downstream_dependencies(dataset_id)
        .await
        .collect()
        .await
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
