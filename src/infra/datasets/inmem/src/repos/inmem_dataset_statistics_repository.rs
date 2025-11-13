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
use database_common::PaginationOpts;
use dill::*;
use internal_error::{InternalError, ResultIntoInternal};
use kamu_datasets::{
    DatasetEntryRemovalListener,
    DatasetEntryRepository,
    DatasetStatistics,
    DatasetStatisticsNotFoundError,
    DatasetStatisticsRepository,
    GetDatasetStatisticsError,
    SetDatasetStatisticsError,
    TotalStatistic,
};

use crate::InMemoryDatasetEntryRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    statistics: HashMap<odf::DatasetID, HashMap<odf::BlockRef, DatasetStatistics>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetStatisticsRepository {
    state: Arc<Mutex<State>>,
    dataset_entry_repository: dill::Lazy<Arc<InMemoryDatasetEntryRepository>>,
}

#[component(pub)]
#[interface(dyn DatasetStatisticsRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetStatisticsRepository {
    pub fn new(dataset_entry_repository: dill::Lazy<Arc<InMemoryDatasetEntryRepository>>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            dataset_entry_repository,
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

    async fn get_total_statistic_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<TotalStatistic, InternalError> {
        let dataset_entry_repository = self.dataset_entry_repository.get().unwrap();
        let owned_datasets_count = dataset_entry_repository
            .dataset_entries_count_by_owner_id(account_id)
            .await?;

        let mut owned_datasets_stream = dataset_entry_repository
            .get_dataset_entries_by_owner_id(
                account_id,
                PaginationOpts {
                    offset: 0,
                    limit: owned_datasets_count,
                },
            )
            .await;
        let mut result = TotalStatistic::default();

        use futures::TryStreamExt;
        while let Some(dataset_entry) = owned_datasets_stream.try_next().await.int_err()? {
            if let Ok(dataset_statistic) = self
                .get_dataset_statistics(&dataset_entry.id, &odf::BlockRef::Head)
                .await
            {
                result.add_dataset_statistic(&dataset_statistic);
            }
        }

        Ok(result)
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
