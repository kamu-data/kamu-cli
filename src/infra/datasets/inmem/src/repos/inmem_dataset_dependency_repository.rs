// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use database_common::{TransactionId, TransactionRef};
use dill::*;
use internal_error::InternalError;
use kamu_datasets::*;

use super::InMemoryDatasetDependencyState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetDependencyRepository {
    state: Arc<InMemoryDatasetDependencyState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn DatasetDependencyRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
impl InMemoryDatasetDependencyRepository {
    pub fn new(
        state: Arc<InMemoryDatasetDependencyState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDependencyRepository for InMemoryDatasetDependencyRepository {
    async fn stores_any_dependencies(&self) -> Result<bool, InternalError> {
        Ok(self
            .state
            .edges
            .read(self.transaction_id, |edges| !edges.is_empty()))
    }

    fn list_all_dependencies(&self) -> DatasetDependenciesIDStream<'_> {
        let dependencies: Vec<_> = self.state.edges.read(self.transaction_id, |edges| {
            let mut upstreams_by_downstream: BTreeMap<&odf::DatasetID, Vec<odf::DatasetID>> =
                BTreeMap::new();
            for ((downstream_dataset_id, upstream_dataset_id), ()) in edges.iter() {
                upstreams_by_downstream
                    .entry(downstream_dataset_id)
                    .or_default()
                    .push(upstream_dataset_id.clone());
            }
            upstreams_by_downstream
                .into_iter()
                .map(|(downstream_dataset_id, mut upstream_dataset_ids)| {
                    upstream_dataset_ids.sort();
                    Ok(DatasetDependencies {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                        upstream_dataset_ids,
                    })
                })
                .collect()
        });

        Box::pin(tokio_stream::iter(dependencies))
    }

    async fn add_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        new_upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), AddDependenciesError> {
        self.state.edges.write(self.transaction_id, |edges| {
            let new_edges: Vec<_> = new_upstream_dataset_ids
                .iter()
                .map(|upstream_dataset_id| {
                    (
                        downstream_dataset_id.clone(),
                        (*upstream_dataset_id).clone(),
                    )
                })
                .collect();
            let view = edges.view();
            let mut requested = HashSet::new();
            // An edge stored before, or requested twice, is a duplicate
            if new_edges
                .iter()
                .any(|edge| view.contains_key(edge) || !requested.insert(edge))
            {
                return Err(AddDependenciesError::Duplicate(
                    AddDependencyDuplicateError {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                    },
                ));
            }

            for edge in new_edges {
                edges.put(edge, ());
            }
            Ok(())
        })
    }

    async fn remove_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), RemoveDependenciesError> {
        self.state.edges.write(self.transaction_id, |edges| {
            let removed_edges: Vec<_> = upstream_dataset_ids
                .iter()
                .map(|upstream_dataset_id| {
                    (
                        downstream_dataset_id.clone(),
                        (*upstream_dataset_id).clone(),
                    )
                })
                .collect();
            let view = edges.view();
            if removed_edges.iter().any(|edge| !view.contains_key(edge)) {
                return Err(RemoveDependenciesError::NotFound(
                    RemoveDependencyMissingError {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                    },
                ));
            }

            for edge in removed_edges {
                edges.delete(edge);
            }
            Ok(())
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetDependencyRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.state.edges.write(self.transaction_id, |edges| {
            edges.delete_where(|(downstream_dataset_id, upstream_dataset_id)| {
                downstream_dataset_id == dataset_id || upstream_dataset_id == dataset_id
            });
        });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
