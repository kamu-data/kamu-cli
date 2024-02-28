// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use kamu_core::{DependencyGraphService, InternalError, ResultIntoInternal};
use kamu_flow_system::{
    DatasetFlowType,
    FlowID,
    FlowKey,
    FlowKeyDataset,
    FlowResult,
    FlowTrigger,
    FlowTriggerInputDatasetFlow,
};

use super::{FlowServiceCallbacksFacade, FlowTypeSupportStrategy};

/////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct DatasetUpdateStrategy {
    dependency_graph_service: Arc<dyn DependencyGraphService>,
}

/////////////////////////////////////////////////////////////////////////////////////////

impl DatasetUpdateStrategy {
    pub fn new(dependency_graph_service: Arc<dyn DependencyGraphService>) -> Self {
        Self {
            dependency_graph_service,
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl FlowTypeSupportStrategy for DatasetUpdateStrategy {
    async fn on_flow_success(
        &self,
        callbacks_facade: &dyn FlowServiceCallbacksFacade,
        success_time: DateTime<Utc>,
        flow_id: FlowID,
        flow_key: &FlowKey,
        flow_result: &FlowResult,
    ) -> Result<(), InternalError> {
        if let FlowKey::Dataset(fk_dataset) = flow_key {
            // Extract list of downstream 1 level datasets
            let dependent_dataset_ids: Vec<_> = self
                .dependency_graph_service
                .get_downstream_dependencies(&fk_dataset.dataset_id)
                .await
                .int_err()?
                .collect()
                .await;

            // For each, scan if flows configurations are on
            for dependent_dataset_id in dependent_dataset_ids {
                let maybe_batching_rule = callbacks_facade.try_get_dataset_batching_rule(
                    &dependent_dataset_id,
                    DatasetFlowType::ExecuteTransform,
                );

                // When dependent flow batching rule is enabled, schedule dependent update
                if let Some(batching_rule) = maybe_batching_rule {
                    let dependent_flow_key = FlowKeyDataset::new(
                        dependent_dataset_id.clone(),
                        DatasetFlowType::ExecuteTransform,
                    )
                    .into();

                    let trigger = FlowTrigger::InputDatasetFlow(FlowTriggerInputDatasetFlow {
                        dataset_id: fk_dataset.dataset_id.clone(),
                        flow_type: fk_dataset.flow_type,
                        flow_id,
                        flow_result: flow_result.clone(),
                    });

                    callbacks_facade
                        .trigger_flow(
                            success_time,
                            &dependent_flow_key,
                            trigger,
                            Some(&batching_rule),
                        )
                        .await
                        .int_err()?;
                }
            }

            Ok(())
        } else {
            unreachable!("Not expecting other types of flow keys than dataset");
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
