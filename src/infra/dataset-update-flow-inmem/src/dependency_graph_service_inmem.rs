// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dill::{component, scope, Singleton};
use kamu_dataset_update_flow::*;
use opendatafabric::DatasetID;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphServiceInMemory {
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

struct State {
    datasets_graph: StableDiGraph<DatasetID, ()>,
    dataset_node_indices: HashMap<DatasetID, NodeIndex>,
}

impl State {
    pub fn new() -> Self {
        Self {
            datasets_graph: StableDiGraph::default(),
            dataset_node_indices: HashMap::new(),
        }
    }

    fn get_dataset_node(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<NodeIndex, DatasetNodeNotFoundError> {
        match self.dataset_node_indices.get(dataset_id) {
            Some(index) => Ok(index.to_owned()),
            None => Err(DatasetNodeNotFoundError {
                dataset_id: dataset_id.clone(),
            }),
        }
    }

    fn get_or_create_dataset_node(&mut self, dataset_id: &DatasetID) -> NodeIndex {
        match self.dataset_node_indices.get(dataset_id) {
            Some(index) => index.to_owned(),
            None => self.datasets_graph.add_node(dataset_id.clone()),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[scope(Singleton)]
impl DependencyGraphServiceInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DependencyGraphService for DependencyGraphServiceInMemory {
    /// Iterates over 1st level of dataset's downstream dependencies
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDownstreamDependenciesError> {
        let downstream_node_datasets: Vec<_> = {
            let state = self.state.lock().unwrap();

            let node_index = state
                .get_dataset_node(dataset_id)
                .map_err(|e| GetDownstreamDependenciesError::DatasetNotFound(e))?;

            state
                .datasets_graph
                .neighbors(node_index)
                .map(|node_index| {
                    state
                        .datasets_graph
                        .node_weight(node_index)
                        .unwrap()
                        .clone()
                })
                .collect()
        };

        Ok(Box::pin(tokio_stream::iter(downstream_node_datasets)))
    }

    /// Tracks a dependency between upstream and downstream dataset
    ///
    /// TODO: connect to event bus
    async fn add_dependency(
        &self,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) -> Result<(), AddDependencyError> {
        let mut state = self.state.lock().unwrap();

        let upstream_node_index = state.get_or_create_dataset_node(dataset_upstream_id);
        let downstream_node_index = state.get_or_create_dataset_node(dataset_downstream_id);
        state
            .datasets_graph
            .update_edge(upstream_node_index, downstream_node_index, ());

        Ok(())
    }

    /// Removes tracked dependency between updstream and downstream dataset
    ///
    /// TODO: connect to event bus
    async fn remove_dependency(
        &self,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) -> Result<(), RemoveDependencyError> {
        let mut state = self.state.lock().unwrap();

        let upstream_node_index = state.get_or_create_dataset_node(dataset_upstream_id);
        let downstream_node_index = state.get_or_create_dataset_node(dataset_downstream_id);

        if let Some(edge_index) = state
            .datasets_graph
            .find_edge(upstream_node_index, downstream_node_index)
        {
            state.datasets_graph.remove_edge(edge_index);
            Ok(())
        } else {
            Err(RemoveDependencyError::NotFound(
                DependencyEdgeNotFoundError {
                    dataset_upstream_id: dataset_upstream_id.clone(),
                    dataset_downstream_id: dataset_downstream_id.clone(),
                },
            ))
        }
    }

    /// Removes dataset node and downstream nodes completely
    async fn remove_dataset(&self, dataset_id: &DatasetID) -> Result<(), RemoveDatasetError> {
        let mut state = self.state.lock().unwrap();

        let node_index = state
            .get_dataset_node(dataset_id)
            .map_err(|e| RemoveDatasetError::DatasetNotFound(e))?;

        state.datasets_graph.remove_node(node_index);
        state.dataset_node_indices.remove(dataset_id);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
