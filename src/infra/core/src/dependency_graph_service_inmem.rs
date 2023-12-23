// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use dill::*;
use event_bus::AsyncEventHandler;
use internal_error::InternalError;
use kamu_core::events::{
    DatasetEventCreated,
    DatasetEventDeleted,
    DatasetEventDependenciesUpdated,
};
use kamu_core::*;
use opendatafabric::DatasetID;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::Direction;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphServiceInMemory {
    repository: Option<Arc<dyn DependencyGraphRepository>>,
    state: Arc<tokio::sync::Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    datasets_graph: StableDiGraph<DatasetID, ()>,
    dataset_node_indices: HashMap<DatasetID, NodeIndex>,
    initially_scanned: bool,
}

impl State {
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
            None => {
                let node_index = self.datasets_graph.add_node(dataset_id.clone());
                self.dataset_node_indices
                    .insert(dataset_id.clone(), node_index);
                node_index
            }
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DependencyGraphService)]
#[interface(dyn AsyncEventHandler<DatasetEventCreated>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<DatasetEventDependenciesUpdated>)]
#[scope(Singleton)]
impl DependencyGraphServiceInMemory {
    pub fn new(repository: Option<Arc<dyn DependencyGraphRepository>>) -> Self {
        Self {
            repository,
            state: Arc::new(tokio::sync::Mutex::new(State::default())),
        }
    }

    async fn ensure_datasets_initially_scanned(&self) -> Result<(), InternalError> {
        let mut state = self.state.lock().await;
        if state.initially_scanned {
            return Ok(());
        }

        self.ensure_datasets_initially_scanned_with(
            &mut state,
            self.repository
                .as_ref()
                .expect("Dependencies graph repository not present")
                .as_ref(),
        )
        .await
    }

    #[tracing::instrument(level = "debug", skip_all)]
    async fn ensure_datasets_initially_scanned_with(
        &self,
        state: &mut State,
        repository: &dyn DependencyGraphRepository,
    ) -> Result<(), InternalError> {
        use tokio_stream::StreamExt;

        let mut dependencies_stream = repository.list_dependencies_of_all_datasets();
        while let Some(Ok((dataset_id, upstream_dataset_id))) = dependencies_stream.next().await {
            self.add_dependency(state, &upstream_dataset_id, &dataset_id);
        }

        state.initially_scanned = true;

        tracing::debug!(
            num_nodes = % state.datasets_graph.node_count(),
            num_edges = % state.datasets_graph.edge_count(),
            "Dependencies graph initialization stats",
        );

        Ok(())
    }

    /// Tracks a dependency between upstream and downstream dataset
    #[tracing::instrument(level = "trace", skip_all, fields(%dataset_upstream_id, %dataset_downstream_id))]
    fn add_dependency(
        &self,
        state: &mut State,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) {
        tracing::debug!(downstream=%dataset_downstream_id, upstream=%dataset_upstream_id, "Adding dataset dependency");

        let upstream_node_index = state.get_or_create_dataset_node(dataset_upstream_id);
        let downstream_node_index = state.get_or_create_dataset_node(dataset_downstream_id);
        state
            .datasets_graph
            .update_edge(upstream_node_index, downstream_node_index, ());
    }

    /// Removes tracked dependency between updstream and downstream dataset
    #[tracing::instrument(level = "trace", skip_all, fields(%dataset_upstream_id, %dataset_downstream_id))]
    fn remove_dependency(
        &self,
        state: &mut State,
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
    ) {
        tracing::debug!(downstream=%dataset_downstream_id, upstream=%dataset_upstream_id, "Removing dataset dependency");

        let upstream_node_index = state.get_or_create_dataset_node(dataset_upstream_id);
        let downstream_node_index = state.get_or_create_dataset_node(dataset_downstream_id);

        // Idempotent DELETE - ignore, if not found
        if let Some(edge_index) = state
            .datasets_graph
            .find_edge(upstream_node_index, downstream_node_index)
        {
            state.datasets_graph.remove_edge(edge_index);
        }
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DependencyGraphService for DependencyGraphServiceInMemory {
    /// Forces initialization of graph data, if it wasn't initialized already.
    /// Ignored if called multiple times
    #[tracing::instrument(level = "debug", skip_all)]
    async fn eager_initialization(
        &self,
        repository: &dyn DependencyGraphRepository,
    ) -> Result<(), InternalError> {
        let mut state = self.state.lock().await;
        if state.initially_scanned {
            return Ok(());
        }

        self.ensure_datasets_initially_scanned_with(&mut state, repository)
            .await
    }

    /// Iterates over 1st level of dataset's downstream dependencies
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDownstreamDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(|e| GetDownstreamDependenciesError::Internal(e))?;

        let downstream_node_datasets: Vec<_> = {
            let state = self.state.lock().await;

            let node_index = state
                .get_dataset_node(dataset_id)
                .map_err(|e| GetDownstreamDependenciesError::DatasetNotFound(e))?;

            state
                .datasets_graph
                .neighbors_directed(node_index, Direction::Outgoing)
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

    /// Iterates over 1st level of dataset's upstream dependencies
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_upstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetUpstreamDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(|e| GetUpstreamDependenciesError::Internal(e))?;

        let upstream_node_datasets: Vec<_> = {
            let state = self.state.lock().await;

            let node_index = state
                .get_dataset_node(dataset_id)
                .map_err(|e| GetUpstreamDependenciesError::DatasetNotFound(e))?;

            state
                .datasets_graph
                .neighbors_directed(node_index, Direction::Incoming)
                .map(|node_index| {
                    state
                        .datasets_graph
                        .node_weight(node_index)
                        .unwrap()
                        .clone()
                })
                .collect()
        };

        Ok(Box::pin(tokio_stream::iter(upstream_node_datasets)))
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventCreated> for DependencyGraphServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &DatasetEventCreated) -> Result<(), InternalError> {
        let mut state = self.state.lock().await;
        state.get_or_create_dataset_node(&event.dataset_id);
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for DependencyGraphServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut state = self.state.lock().await;

        let node_index = state
            .get_dataset_node(&event.dataset_id)
            .map_err(|e| e.int_err())?;

        state.datasets_graph.remove_node(node_index);
        state.dataset_node_indices.remove(&event.dataset_id);

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDependenciesUpdated> for DependencyGraphServiceInMemory {
    #[tracing::instrument(level = "debug", skip_all, fields(?event))]
    async fn handle(&self, event: &DatasetEventDependenciesUpdated) -> Result<(), InternalError> {
        let mut state = self.state.lock().await;

        let node_index = state
            .get_dataset_node(&event.dataset_id)
            .map_err(|e| e.int_err())?;

        let existing_upstream_ids: HashSet<_> = state
            .datasets_graph
            .neighbors_directed(node_index, Direction::Incoming)
            .map(|node_index| {
                state
                    .datasets_graph
                    .node_weight(node_index)
                    .unwrap()
                    .clone()
            })
            .collect();

        let new_upstream_ids: HashSet<_> =
            HashSet::from_iter(event.new_upstream_ids.iter().cloned());

        for obsolete_upstream_id in existing_upstream_ids.difference(&new_upstream_ids) {
            self.remove_dependency(&mut state, obsolete_upstream_id, &event.dataset_id)
        }

        for added_id in new_upstream_ids.difference(&existing_upstream_ids) {
            self.add_dependency(&mut state, added_id, &event.dataset_id);
        }

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
