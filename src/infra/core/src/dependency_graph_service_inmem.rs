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
use kamu_core::*;
use messaging_outbox::{
    MessageConsumer,
    MessageConsumerMeta,
    MessageConsumerT,
    MessageConsumptionDurability,
};
use opendatafabric::DatasetID;
use petgraph::stable_graph::{NodeIndex, StableDiGraph};
use petgraph::visit::{depth_first_search, Bfs, DfsEvent, Reversed};
use petgraph::Direction;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DependencyGraphServiceInMemory {
    repository: Option<Arc<dyn DependencyGraphRepository>>,
    state: Arc<tokio::sync::RwLock<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DependencyGraphService)]
#[interface(dyn MessageConsumer)]
#[interface(dyn MessageConsumerT<DatasetLifecycleMessage>)]
#[meta(MessageConsumerMeta {
    consumer_name: MESSAGE_CONSUMER_KAMU_CORE_DEPENDENCY_GRAPH_SERVICE,
    feeding_producers: &[MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE],
    durability: MessageConsumptionDurability::BestEffort,
 })]
#[scope(Singleton)]
impl DependencyGraphServiceInMemory {
    pub fn new(repository: Option<Arc<dyn DependencyGraphRepository>>) -> Self {
        Self {
            repository,
            state: Arc::new(tokio::sync::RwLock::new(State::default())),
        }
    }

    async fn ensure_datasets_initially_scanned(&self) -> Result<(), InternalError> {
        let mut state = self.state.write().await;
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

        while let Some(Ok(dataset_dependencies)) = dependencies_stream.next().await {
            let DatasetDependencies {
                downstream_dataset_id,
                upstream_dataset_ids,
            } = dataset_dependencies;

            if !upstream_dataset_ids.is_empty() {
                for upstream_dataset_id in upstream_dataset_ids {
                    self.add_dependency(state, &upstream_dataset_id, &downstream_dataset_id);
                }
            } else {
                state.get_or_create_dataset_node(&downstream_dataset_id);
            }
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
        dataset_upstream_id: &DatasetID,
        dataset_downstream_id: &DatasetID,
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
        dataset_ids: &[DatasetID],
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
        dataset_ids: Vec<DatasetID>,
    ) -> Result<Vec<DatasetID>, GetDependenciesError> {
        let state = self.state.read().await;

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
        dataset_ids: Vec<DatasetID>,
    ) -> Result<Vec<DatasetID>, GetDependenciesError> {
        let state = self.state.read().await;

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
impl DependencyGraphService for DependencyGraphServiceInMemory {
    /// Forces initialization of graph data, if it wasn't initialized already.
    /// Ignored if called multiple times
    #[tracing::instrument(level = "debug", skip_all)]
    async fn eager_initialization(
        &self,
        repository: &dyn DependencyGraphRepository,
    ) -> Result<(), InternalError> {
        let mut state = self.state.write().await;
        if state.initially_scanned {
            return Ok(());
        }

        self.ensure_datasets_initially_scanned_with(&mut state, repository)
            .await
    }

    async fn get_recursive_upstream_dependencies(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> Result<DatasetIDStream, GetDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(GetDependenciesError::Internal)
            .unwrap();

        let result = self
            .run_recursive_reversed_breadth_first_search(dataset_ids)
            .await?;

        Ok(Box::pin(tokio_stream::iter(result)))
    }

    async fn get_recursive_downstream_dependencies(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> Result<DatasetIDStream, GetDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(GetDependenciesError::Internal)
            .unwrap();

        let result = self.run_recursive_depth_first_search(dataset_ids).await?;

        Ok(Box::pin(tokio_stream::iter(result)))
    }

    /// Iterates over 1st level of dataset's downstream dependencies
    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_id))]
    async fn get_downstream_dependencies(
        &self,
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(GetDependenciesError::Internal)?;

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
        dataset_id: &DatasetID,
    ) -> Result<DatasetIDStream, GetDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(GetDependenciesError::Internal)?;

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

    async fn in_dependency_order(
        &self,
        dataset_ids: Vec<DatasetID>,
        order: DependencyOrder,
    ) -> Result<Vec<DatasetID>, GetDependenciesError> {
        self.ensure_datasets_initially_scanned()
            .await
            .int_err()
            .map_err(GetDependenciesError::Internal)
            .unwrap();

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

impl MessageConsumer for DependencyGraphServiceInMemory {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl MessageConsumerT<DatasetLifecycleMessage> for DependencyGraphServiceInMemory {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        name = "DependencyGraphServiceInMemory[DatasetLifecycleMessage]"
    )]
    async fn consume_message(
        &self,
        _: &Catalog,
        message: &DatasetLifecycleMessage,
    ) -> Result<(), InternalError> {
        tracing::debug!(received_message = ?message, "Received dataset lifecycle message");

        let mut state = self.state.write().await;

        match message {
            DatasetLifecycleMessage::Created(message) => {
                state.get_or_create_dataset_node(&message.dataset_id);
            }

            DatasetLifecycleMessage::Deleted(message) => {
                let node_index = state.get_dataset_node(&message.dataset_id).int_err()?;

                state.datasets_graph.remove_node(node_index);
                state.dataset_node_indices.remove(&message.dataset_id);
            }

            DatasetLifecycleMessage::DependenciesUpdated(message) => {
                let node_index = state.get_dataset_node(&message.dataset_id).int_err()?;

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
                    message.new_upstream_ids.iter().cloned().collect();

                for obsolete_upstream_id in existing_upstream_ids.difference(&new_upstream_ids) {
                    self.remove_dependency(&mut state, obsolete_upstream_id, &message.dataset_id);
                }

                for added_id in new_upstream_ids.difference(&existing_upstream_ids) {
                    self.add_dependency(&mut state, added_id, &message.dataset_id);
                }
            }

            DatasetLifecycleMessage::Renamed(_) => {
                // No action required
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
