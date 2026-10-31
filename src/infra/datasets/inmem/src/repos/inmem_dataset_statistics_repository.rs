// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use cheap_clone::CheapClone;
use database_common::{TransactionId, TransactionRef};
use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    DatasetEntryRemovalListener,
    DatasetStatistics,
    DatasetStatisticsNotFoundError,
    DatasetStatisticsRepository,
    GetDatasetStatisticsError,
    SetDatasetStatisticsError,
    TotalStatistic,
};

use super::InMemoryDatasetStatisticsState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetStatisticsRepository {
    state: Arc<InMemoryDatasetStatisticsState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn DatasetStatisticsRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
impl InMemoryDatasetStatisticsRepository {
    pub fn new(
        state: Arc<InMemoryDatasetStatisticsState>,
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
impl DatasetStatisticsRepository for InMemoryDatasetStatisticsRepository {
    async fn has_any_stats(&self) -> Result<bool, InternalError> {
        Ok(self
            .state
            .statistics
            .read(self.transaction_id, |statistics| !statistics.is_empty()))
    }

    async fn get_total_statistic_by_account_id(
        &self,
        _account_id: &odf::AccountID,
    ) -> Result<TotalStatistic, InternalError> {
        // TODO: this is a dummy implementation that does not filter by account_id
        // To implement real logic requires to handle issues with transaction leaks
        Ok(self
            .state
            .statistics
            .read(self.transaction_id, |statistics| {
                let mut total = TotalStatistic::default();
                for (_, stat) in statistics.iter() {
                    total.add_dataset_statistic(stat);
                }
                total
            }))
    }

    async fn get_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<DatasetStatistics, GetDatasetStatisticsError> {
        self.state
            .statistics
            .read(self.transaction_id, |statistics| {
                statistics
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .copied()
            })
            .ok_or_else(|| {
                GetDatasetStatisticsError::NotFound(DatasetStatisticsNotFoundError {
                    dataset_id: dataset_id.clone(),
                    block_ref: block_ref.cheap_clone(),
                })
            })
    }

    async fn set_dataset_statistics(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        statistics: DatasetStatistics,
    ) -> Result<(), SetDatasetStatisticsError> {
        self.state.statistics.write(self.transaction_id, |stored| {
            stored.put((dataset_id.clone(), block_ref.cheap_clone()), statistics);
        });
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
        self.state
            .statistics
            .write(self.transaction_id, |statistics| {
                statistics.delete_where(|(id, _)| id == dataset_id);
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
