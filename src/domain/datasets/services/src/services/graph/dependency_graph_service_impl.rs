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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::{
    DatasetNodeNotFoundError,
    DatasetRegistry,
    DependencyDatasetIDStream,
    DependencyGraphService,
    DependencyOrder,
    GetDependenciesError,
};
use kamu_datasets::{DatasetDependencies, DatasetDependencyRepository};
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{depth_first_search, Bfs, DfsEvent, Reversed};
use petgraph::Direction;

use super::DependencyGraphWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphServiceImpl {
    state: Arc<tokio::sync::RwLock<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    datasets_graph: StableDiGraph<odf::DatasetID, ()>,
    dataset_node_indices: HashMap<odf::DatasetID, NodeIndex>,
}

impl State {
    fn get_dataset_node(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<NodeIndex, DatasetNodeNotFoundError> {
        match self.dataset_node_indices.get(dataset_id) {
            Some(index) => Ok(index.to_owned()),
            None => Err(DatasetNodeNotFoundError {
                dataset_id: dataset_id.clone(),
            }),
        }
    }

    fn get_or_create_dataset_node(&mut self, dataset_id: &odf::DatasetID) -> NodeIndex {
        match self.dataset_node_indices.get(dataset_id) {
            Some(index) => index.to_owned(),
            None => {
                let node_index = self.datasets_graph.add_node(dataset_id.clone());
                self.dataset_node_indices
                    .insert(dataset_id.clone(), node_index);
                tracing::debug!(%dataset_id, "Inserted new dependency graph node");
                node_index
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DependencyGraphService)]
#[interface(dyn DependencyGraphWriter)]
#[scope(Singleton)]
impl DependencyGraphServiceImpl {
    pub fn new() -> Self {
        Self {
            state: Arc::new(tokio::sync::RwLock::new(State::default())),
        }
    }

    #[tracing::instrument(level = "debug", skip_all)]
    pub async fn load_dependency_graph(
        &self,
        dataset_registry: &dyn DatasetRegistry,
        dependency_repository: &dyn DatasetDependencyRepository,
    ) -> Result<(), InternalError> {
        use tokio_stream::StreamExt;

        let mut state = self.state.write().await;
        assert!(state.datasets_graph.node_count() == 0 && state.datasets_graph.edge_count() == 0);

        tracing::debug!("Restoring dataset nodes in dependency graph");

        let mut datasets_stream = dataset_registry.all_dataset_handles();
        while let Some(Ok(dataset_handle)) = datasets_stream.next().await {
            state.get_or_create_dataset_node(&dataset_handle.id);
        }

        tracing::debug!("Restoring dependency graph edges");

        let mut dependencies_stream = dependency_repository.list_all_dependencies();
        while let Some(Ok(dataset_dependencies)) = dependencies_stream.next().await {
            let DatasetDependencies {
                downstream_dataset_id,
                upstream_dataset_ids,
            } = dataset_dependencies;

            if !upstream_dataset_ids.is_empty() {
                for upstream_dataset_id in upstream_dataset_ids {
                    self.add_dependency(&mut state, &upstream_dataset_id, &downstream_dataset_id);
                }
            } else {
                state.get_or_create_dataset_node(&downstream_dataset_id);
            }
        }

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
        dataset_upstream_id: &odf::DatasetID,
        dataset_downstream_id: &odf::DatasetID,
    ) {
        tracing::debug!(
            downstream = %dataset_downstream_id,
            upstream = %dataset_upstream_id,
            "Adding dataset dependency"
        );

        let upstream_node_index = state.get_or_create_dataset_node(dataset_upstream_id);
        let downstream_node_index = state.get_or_create_dataset_node(dataset_downstream_id);
        state
            .datasets_graph
            .update_edge(upstream_node_index, downstream_node_index, ());
    }

    /// Removes tracked dependency between upstream and downstream dataset
    #[tracing::instrument(level = "trace", skip_all, fields(%dataset_upstream_id, %dataset_downstream_id))]
    fn remove_dependency(
        &self,
        state: &mut State,
        dataset_upstream_id: &odf::DatasetID,
        dataset_downstream_id: &odf::DatasetID,
    ) {
        tracing::debug!(
            downstream = %dataset_downstream_id,
            upstream = %dataset_upstream_id,
            "Removing dataset dependency"
        );

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

    fn get_nodes_from_dataset_ids(
        &self,
        dataset_ids: &[odf::DatasetID],
        state: &State,
    ) -> Result<Vec<NodeIndex>, GetDependenciesError> {
        dataset_ids
            .iter()
            .map(|dataset_id| {
                state
                    .get_dataset_node(dataset_id)
                    .map_err(GetDependenciesError::DatasetNotFound)
            })
            .collect()
    }

    async fn run_recursive_reversed_breadth_first_search(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<Vec<odf::DatasetID>, GetDependenciesError> {
        let state = self.state.read().await;

        tracing::debug!(
            num_nodes = % state.datasets_graph.node_count(),
            num_edges = % state.datasets_graph.edge_count(),
            "Graph state before breadth first search"
        );

        let reversed_graph = Reversed(&state.datasets_graph);
        let nodes_to_search = self.get_nodes_from_dataset_ids(&dataset_ids, &state)?;

        let mut result = Vec::new();
        let mut visited_indexes = HashSet::new();

        for node_index in &nodes_to_search {
            let mut bfs = Bfs::new(&reversed_graph, *node_index);
            let mut search_node_result = vec![];

            while let Some(current_node_index) = bfs.next(&reversed_graph) {
                if !visited_indexes.contains(&current_node_index) {
                    visited_indexes.insert(current_node_index);
                    search_node_result.push(
                        reversed_graph
                            .0
                            .node_weight(current_node_index)
                            .unwrap()
                            .clone(),
                    );
                }
            }
            search_node_result.reverse();
            result.extend(search_node_result);
        }

        Ok(result)
    }

    async fn run_recursive_depth_first_search(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<Vec<odf::DatasetID>, GetDependenciesError> {
        let state = self.state.read().await;

        tracing::debug!(
            num_nodes = % state.datasets_graph.node_count(),
            num_edges = % state.datasets_graph.edge_count(),
            "Graph state before depth first search"
        );

        let nodes_to_search = self.get_nodes_from_dataset_ids(&dataset_ids, &state)?;

        let mut result = vec![];

        depth_first_search(&state.datasets_graph, nodes_to_search, |event| {
            // Postorder nodes nodes
            if let DfsEvent::Finish(node_index, _) = event {
                result.push(
                    state
                        .datasets_graph
                        .node_weight(node_index)
                        .unwrap()
                        .clone(),
                );
            }
        });

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DependencyGraphService for DependencyGraphServiceImpl {
    async fn get_recursive_upstream_dependencies(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError> {
        let result = self
            .run_recursive_reversed_breadth_first_search(dataset_ids)
            .await?;

        Ok(Box::pin(tokio_stream::iter(result)))
    }

    async fn get_recursive_downstream_dependencies(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError> {
        let result = self.run_recursive_depth_first_search(dataset_ids).await?;

        Ok(Box::pin(tokio_stream::iter(result)))
    }

    /// Iterates over 1st level of dataset's downstream dependencies
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError> {
        let downstream_node_datasets: Vec<_> = {
            let state = self.state.read().await;

            let node_index = state
                .get_dataset_node(dataset_id)
                .map_err(GetDependenciesError::DatasetNotFound)?;

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
        dataset_id: &odf::DatasetID,
    ) -> Result<DependencyDatasetIDStream, GetDependenciesError> {
        let upstream_node_datasets: Vec<_> = {
            let state = self.state.read().await;

            let node_index = state
                .get_dataset_node(dataset_id)
                .map_err(GetDependenciesError::DatasetNotFound)?;

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

    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_ids, ?order))]
    async fn in_dependency_order(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
        order: DependencyOrder,
    ) -> Result<Vec<odf::DatasetID>, GetDependenciesError> {
        let original_set: std::collections::HashSet<_> = dataset_ids.iter().cloned().collect();

        let mut result = match order {
            DependencyOrder::BreadthFirst => {
                self.run_recursive_reversed_breadth_first_search(dataset_ids)
                    .await?
            }
            DependencyOrder::DepthFirst => {
                self.run_recursive_depth_first_search(dataset_ids).await?
            }
        };

        result.retain(|id| original_set.contains(id));

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DependencyGraphWriter for DependencyGraphServiceImpl {
    async fn create_dataset_node(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        state.get_or_create_dataset_node(dataset_id);

        Ok(())
    }

    async fn remove_dataset_node(&self, dataset_id: &odf::DatasetID) -> Result<(), InternalError> {
        let mut state = self.state.write().await;

        let node_index = state.get_dataset_node(dataset_id).int_err()?;

        state.datasets_graph.remove_node(node_index);
        state.dataset_node_indices.remove(dataset_id);

        Ok(())
    }

    async fn update_dataset_node_dependencies(
        &self,
        catalog: &Catalog,
        dataset_id: &odf::DatasetID,
        new_upstream_ids: Vec<odf::DatasetID>,
    ) -> Result<(), InternalError> {
        let repository = catalog
            .get_one::<dyn DatasetDependencyRepository>()
            .unwrap();

        let mut state = self.state.write().await;

        let node_index = state.get_dataset_node(dataset_id).int_err()?;

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

        let new_upstream_ids: HashSet<_> = new_upstream_ids.iter().cloned().collect();

        let obsolete_dependencies: Vec<_> = existing_upstream_ids
            .difference(&new_upstream_ids)
            .collect();
        let added_dependencies: Vec<_> = new_upstream_ids
            .difference(&existing_upstream_ids)
            .collect();

        repository
            .remove_upstream_dependencies(dataset_id, &obsolete_dependencies)
            .await
            .int_err()?;

        repository
            .add_upstream_dependencies(dataset_id, &added_dependencies)
            .await
            .int_err()?;

        for obsolete_upstream_id in obsolete_dependencies {
            self.remove_dependency(&mut state, obsolete_upstream_id, dataset_id);
        }

        for added_id in added_dependencies {
            self.add_dependency(&mut state, added_id, dataset_id);
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
