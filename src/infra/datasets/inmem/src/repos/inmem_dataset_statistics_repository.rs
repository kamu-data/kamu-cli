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

use cheap_clone::CheapClone;
use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    DatasetEntryRemovalListener,
    DatasetStatistics,
    DatasetStatisticsNotFoundError,
    DatasetStatisticsRepository,
    GetDatasetStatisticsError,
    SetDatasetStatisticsError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    statistics: HashMap<odf::DatasetID, HashMap<odf::BlockRef, DatasetStatistics>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetStatisticsRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetStatisticsRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetStatisticsRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetStatisticsRepository for InMemoryDatasetStatisticsRepository {
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(!guard.statistics.is_empty())
    }

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError> {
        let guard = self.state.lock().unwrap();
        if let Some(stats_by_ref) = guard.statistics.get(dataset_id)
            && let Some(stats) = stats_by_ref.get(block_ref)
        {
            return Ok(*stats);
        }

        Err(GetDatasetStatisticsError::NotFound(
            DatasetStatisticsNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: block_ref.cheap_clone(),
            },
        ))
    }

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError> {
        let mut guard = self.state.lock().unwrap();
        guard
            .statistics
            .entry(dataset_id.clone())
            .or_default()
            .insert(block_ref.cheap_clone(), statistics);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetStatisticsRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.statistics.remove(dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
