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
use internal_error::InternalError;
use kamu_core::DatasetRegistry;
use kamu_datasets::*;
use messaging_outbox::*;
use petgraph::Direction;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{Bfs, DfsEvent, Reversed, depth_first_search};

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
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[interface(dyn MessageConsumerT<DatasetDependenciesMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    feeding_producers: &[
        MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
        MESSAGE_PRODUCER_KAMU_DATASET_DEPENDENCY_GRAPH_SERVICE,
    ],
    delivery: MessageDeliveryMechanism::Transactional,
    initial_consumer_boundary: InitialConsumerBoundary::Latest,
})]
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
    ) -> DependencyDatasetIDStream {
        let downstream_node_datasets: Vec<_> = {
            let state = self.state.read().await;

            // Try to get node index, but don't panic if not found
            let node_index = match state.get_dataset_node(dataset_id) {
                Ok(node_index) => node_index,
                Err(DatasetNodeNotFoundError { .. }) => {
                    // Don't panic if dataset not found, just stream empty collection
                    tracing::debug!(%dataset_id, "Dataset not found in dependency graph. Empty dependencies list returned");
                    return Box::pin(tokio_stream::empty());
                }
            };

            // Dataset found, so we can safely get downstream dependencies
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

        Box::pin(tokio_stream::iter(downstream_node_datasets))
    }

    /// Iterates over 1st level of dataset's upstream dependencies
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_upstream_dependencies(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> DependencyDatasetIDStream {
        let upstream_node_datasets: Vec<_> = {
            let state = self.state.read().await;

            // Try to get node index, but don't panic if not found
            let node_index = match state.get_dataset_node(dataset_id) {
                Ok(node_index) => node_index,
                Err(DatasetNodeNotFoundError { .. }) => {
                    // Don't panic if dataset not found, just stream empty collection
                    tracing::debug!(%dataset_id, "Dataset not found in dependency graph. Empty dependencies list returned");
                    return Box::pin(tokio_stream::empty());
                }
            };

            // Dataset found, so we can safely get upstream dependencies
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

        Box::pin(tokio_stream::iter(upstream_node_datasets))
    }

    #[tracing::instrument(level = "debug", skip_all, fields(%upstream_dataset_id, %downstream_dataset_id))]
    async fn dependency_exists(
        &self,
        upstream_dataset_id: &odf::DatasetID,
        downstream_dataset_id: &odf::DatasetID,
    ) -> Result<bool, InternalError> {
        let state = self.state.read().await;

        let Ok(upstream_node_index) = state.get_dataset_node(upstream_dataset_id) else {
            return Ok(false);
        };

        let Ok(downstream_node_index) = state.get_dataset_node(downstream_dataset_id) else {
            return Ok(false);
        };

        Ok(state
            .datasets_graph
            .contains_edge(upstream_node_index, downstream_node_index))
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

impl MessageConsumer for DependencyGraphServiceImpl {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DependencyGraphServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DependencyGraphServiceImpl[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        match message {
            DatasetLifecycleMessage::Created(created) => {
                let mut state = self.state.write().await;

                state.get_or_create_dataset_node(&created.dataset_id);

                Ok(())
            }

            DatasetLifecycleMessage::Deleted(deleted) => {
                let mut state = self.state.write().await;

                match state.get_dataset_node(&deleted.dataset_id) {
                    Ok(node_index) => {
                        state.datasets_graph.remove_node(node_index);
                        state.dataset_node_indices.remove(&deleted.dataset_id);
                    }
                    Err(DatasetNodeNotFoundError { .. }) => {
                        // Don't panic if dataset not found, just ignore
                        tracing::debug!(%deleted.dataset_id, "Dataset not found in dependency graph. No delete action taken");
                    }
                }

                Ok(())
            }

            // No action required
            DatasetLifecycleMessage::Renamed(_) => Ok(()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetDependenciesMessage> for DependencyGraphServiceImpl {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DependencyGraphServiceImpl[DatasetDependenciesMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetDependenciesMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset dependencies message");

        match message {
            DatasetDependenciesMessage::Updated(updated_message) => {
                let mut state = self.state.write().await;

                for removed_upstream_id in &updated_message.removed_upstream_ids {
                    self.remove_dependency(
                        &mut state,
                        removed_upstream_id,
                        &updated_message.dataset_id,
                    );
                }

                for added_id in &updated_message.added_upstream_ids {
                    self.add_dependency(&mut state, added_id, &updated_message.dataset_id);
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
