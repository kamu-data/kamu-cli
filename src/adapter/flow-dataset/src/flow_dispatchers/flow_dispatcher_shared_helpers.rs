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
use kamu_flow_system::{self as fs, FlowTriggerServiceExt};

use crate::{FLOW_TYPE_DATASET_COMPACT, FLOW_TYPE_DATASET_TRANSFORM, FlowConfigRuleCompact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn trigger_transform_flow_for_all_downstream_datasets(
    dependency_graph_service: &dyn DependencyGraphService,
    flow_trigger_service: &dyn fs::FlowTriggerService,
    flow_run_service: &dyn fs::FlowRunService,
    flow_binding: &fs::FlowBinding,
    activation_cause: fs::FlowActivationCause,
) -> Result<(), InternalError> {
    let dataset_id = flow_binding.get_dataset_id_or_die()?;
    let downstream_dataset_ids =
        fetch_downstream_dataset_ids(dependency_graph_service, &dataset_id).await;

    for downstream_dataset_id in downstream_dataset_ids {
        let downstream_binding = fs::FlowBinding::for_dataset(
            downstream_dataset_id.clone(),
            FLOW_TYPE_DATASET_TRANSFORM,
        );
        if let Some(batching_rule) = flow_trigger_service
            .try_get_flow_batching_rule(&downstream_binding)
            .await
            .int_err()?
        {
            flow_run_service
                .run_flow_automatically(
                    &downstream_binding,
                    activation_cause.clone(),
                    Some(fs::FlowTriggerRule::Batching(batching_rule)),
                    None,
                    None,
                )
                .await
                .int_err()?;
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) async fn trigger_hard_compaction_flow_for_own_downstream_datasets(
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
            let downstream_binding =
                fs::FlowBinding::for_dataset(downstream_dataset_id, FLOW_TYPE_DATASET_COMPACT);
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

pub(crate) async fn fetch_downstream_dataset_ids(
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
