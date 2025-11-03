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
use internal_error::InternalError;
use kamu_datasets::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    upstream_by_downstream: HashMap<odf::DatasetID, HashSet<odf::DatasetID>>,
    downstream_by_upstream: HashMap<odf::DatasetID, HashSet<odf::DatasetID>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetDependencyRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetDependencyRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetDependencyRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDependencyRepository for InMemoryDatasetDependencyRepository {
    async fn stores_any_dependencies(&self) -> Result<bool, InternalError> {
        let guard = self.state.lock().unwrap();
        let has_dependencies = !guard.upstream_by_downstream.is_empty();
        Ok(has_dependencies)
    }

    fn list_all_dependencies(&self) -> DatasetDependenciesIDStream<'_> {
        let dependencies: Vec<_> = {
            let guard = self.state.lock().unwrap();
            guard
                .upstream_by_downstream
                .iter()
                .filter(|(_, upstreams)| !upstreams.is_empty())
                .map(|(downstream_dataset_id, upstreams)| {
                    let mut upstream_dataset_ids: Vec<_> = upstreams.iter().cloned().collect();
                    upstream_dataset_ids.sort();

                    Ok(DatasetDependencies {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                        upstream_dataset_ids,
                    })
                })
                .collect()
        };

        Box::pin(tokio_stream::iter(dependencies))
    }

    async fn add_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        new_upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), AddDependenciesError> {
        if new_upstream_dataset_ids.is_empty() {
            return Ok(());
        }

        let mut guard = self.state.lock().unwrap();

        let upstreams = guard
            .upstream_by_downstream
            .entry(downstream_dataset_id.clone())
            .or_default();

        for new_upstream_dataset_id in new_upstream_dataset_ids {
            upstreams.insert((*new_upstream_dataset_id).clone());
        }

        for new_upstream_dataset_id in new_upstream_dataset_ids {
            let downstreams = guard
                .downstream_by_upstream
                .entry((*new_upstream_dataset_id).clone())
                .or_default();
            let inserted = downstreams.insert(downstream_dataset_id.clone());
            if !inserted {
                return Err(AddDependenciesError::Duplicate(
                    AddDependencyDuplicateError {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                    },
                ));
            }
        }

        Ok(())
    }

    async fn remove_upstream_dependencies(
        &self,
        downstream_dataset_id: &odf::DatasetID,
        upstream_dataset_ids: &[&odf::DatasetID],
    ) -> Result<(), RemoveDependenciesError> {
        if upstream_dataset_ids.is_empty() {
            return Ok(());
        }

        let mut guard = self.state.lock().unwrap();

        let maybe_current_upstreams = guard.upstream_by_downstream.get_mut(downstream_dataset_id);
        if let Some(current_upstreams) = maybe_current_upstreams {
            let some_missing = upstream_dataset_ids
                .iter()
                .any(|id| !current_upstreams.contains(*id));
            if some_missing {
                return Err(RemoveDependenciesError::NotFound(
                    RemoveDependencyMissingError {
                        downstream_dataset_id: downstream_dataset_id.clone(),
                    },
                ));
            }

            for removed_upstream_id in upstream_dataset_ids {
                current_upstreams.remove(*removed_upstream_id);
            }

            for removed_upstream_id in upstream_dataset_ids {
                if let Some(downstreams) =
                    guard.downstream_by_upstream.get_mut(*removed_upstream_id)
                {
                    downstreams.remove(downstream_dataset_id);
                }
            }
        } else {
            return Err(RemoveDependenciesError::NotFound(
                RemoveDependencyMissingError {
                    downstream_dataset_id: downstream_dataset_id.clone(),
                },
            ));
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetDependencyRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();

        if let Some(upstreams) = guard.upstream_by_downstream.remove(dataset_id) {
            for upstream_dataset_id in upstreams {
                if let Some(downstreams) =
                    guard.downstream_by_upstream.get_mut(&upstream_dataset_id)
                {
                    downstreams.remove(dataset_id);
                }
            }
        }

        if let Some(downstreams) = guard.downstream_by_upstream.remove(dataset_id) {
            for downstream_dataset_id in downstreams {
                if let Some(upstreams) =
                    guard.upstream_by_downstream.get_mut(&downstream_dataset_id)
                {
                    upstreams.remove(dataset_id);
                }
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
