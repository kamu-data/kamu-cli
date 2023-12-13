// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

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
    state: Arc<Mutex<State>>,
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    datasets_graph: StableDiGraph<DatasetID, ()>,
    dataset_node_indices: HashMap<DatasetID, NodeIndex>,
    status: DependenciesGraphStatus,
}

enum DependenciesGraphStatus {
    NotScanned,
    Scanning,
    Scanned,
}

impl Default for DependenciesGraphStatus {
    fn default() -> Self {
        Self::NotScanned
    }
}

impl State {
    fn reset(&mut self) {
        self.datasets_graph.clear();
        self.dataset_node_indices.clear();
        self.status = DependenciesGraphStatus::NotScanned;
    }

    fn ensure_scanned(&self) -> Result<(), DependenciesNotScannedError> {
        match self.status {
            DependenciesGraphStatus::NotScanned | DependenciesGraphStatus::Scanning => {
                Err(DependenciesNotScannedError {})
            }
            DependenciesGraphStatus::Scanned => Ok(()),
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
#[interface(dyn DependencyGraphServiceInitializer)]
#[interface(dyn AsyncEventHandler<DatasetEventCreated>)]
#[interface(dyn AsyncEventHandler<DatasetEventDeleted>)]
#[interface(dyn AsyncEventHandler<DatasetEventDependenciesUpdated>)]
#[scope(Singleton)]
impl DependencyGraphServiceInMemory {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// Tracks a dependency between upstream and downstream dataset
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
    /// Iterates over 1st level of dataset's downstream dependencies
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDownstreamDependenciesError> {
        let downstream_node_datasets: Vec<_> = {
            let state = self.state.lock().unwrap();
            state
                .ensure_scanned()
                .map_err(|e| GetDownstreamDependenciesError::NotScanned(e))?;

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
    async fn get_upstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetUpstreamDependenciesError> {
        let upstream_node_datasets: Vec<_> = {
            let state = self.state.lock().unwrap();
            state
                .ensure_scanned()
                .map_err(|e| GetUpstreamDependenciesError::NotScanned(e))?;

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
impl DependencyGraphServiceInitializer for DependencyGraphServiceInMemory {
    async fn full_scan(
        &self,
        dataset_repo: &dyn DatasetRepository,
        force: bool,
    ) -> Result<(), DependenciesScanError> {
        use tokio_stream::StreamExt;

        {
            let mut state = self.state.lock().unwrap();
            match &state.status {
                DependenciesGraphStatus::NotScanned => {
                    state.status = DependenciesGraphStatus::Scanning;
                }
                DependenciesGraphStatus::Scanning => {
                    return Err(DependenciesScanError::AlreadyScanning(
                        DependenciesAlreadyScanningError {},
                    ))
                }
                DependenciesGraphStatus::Scanned => {
                    if force {
                        state.reset();
                        state.status = DependenciesGraphStatus::Scanning;
                    } else {
                        return Ok(());
                    }
                }
            }
        }

        let mut datasets_stream = dataset_repo.get_all_datasets();
        while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
            tracing::debug!(dataset=%dataset_handle, "Scanning dataset dependencies");

            let summary = dataset_repo
                .get_dataset(&dataset_handle.as_local_ref())
                .await
                .int_err()?
                .get_summary(GetSummaryOpts::default())
                .await
                .int_err()?;

            let mut state = self.state.lock().unwrap();
            for input_dataset in summary.dependencies.iter() {
                if let Some(input_id) = &input_dataset.id {
                    self.add_dependency(&mut state, input_id, &dataset_handle.id);
                }
            }
        }

        let mut state = self.state.lock().unwrap();
        state.status = DependenciesGraphStatus::Scanned;

        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventCreated> for DependencyGraphServiceInMemory {
    async fn handle(&self, event: &DatasetEventCreated) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();
        state.get_or_create_dataset_node(&event.dataset_id);
        Ok(())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AsyncEventHandler<DatasetEventDeleted> for DependencyGraphServiceInMemory {
    async fn handle(&self, event: &DatasetEventDeleted) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

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
    async fn handle(&self, event: &DatasetEventDependenciesUpdated) -> Result<(), InternalError> {
        let mut state = self.state.lock().unwrap();

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
