// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatasetStatistics {
    pub last_pulled: Option<DateTime<Utc>>,
    pub num_records: u64,
    pub data_size: u64,
    pub checkpoints_size: u64,
    pub num_object_links: u64,
    pub object_links_size: u64,
}

impl DatasetStatistics {
    pub fn get_size_summary(&self) -> Option<u64> {
        calculate_total_size(
            self.data_size,
            self.checkpoints_size,
            self.object_links_size,
        )
    }
}

impl std::ops::Add for DatasetStatistics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            last_pulled: other.last_pulled.or(self.last_pulled),
            num_records: self.num_records + other.num_records,
            data_size: self.data_size + other.data_size,
            checkpoints_size: self.checkpoints_size + other.checkpoints_size,
            num_object_links: self.num_object_links + other.num_object_links,
            object_links_size: self.object_links_size + other.object_links_size,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TotalStatistic {
    pub num_records: u64,
    pub data_size: u64,
    pub checkpoints_size: u64,
    pub num_object_links: u64,
    pub object_links_size: u64,
}

impl TotalStatistic {
    pub fn get_size_summary(&self) -> Option<u64> {
        calculate_total_size(
            self.data_size,
            self.checkpoints_size,
            self.object_links_size,
        )
    }

    pub fn add_dataset_statistic(&mut self, dataset_statistic: &DatasetStatistics) {
        self.num_records += dataset_statistic.num_records;
        self.data_size += dataset_statistic.data_size;
        self.checkpoints_size += dataset_statistic.checkpoints_size;
        self.num_object_links += dataset_statistic.num_object_links;
        self.object_links_size += dataset_statistic.object_links_size;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn calculate_total_size(
    data_size: u64,
    checkpoints_size: u64,
    object_links_size: u64,
) -> Option<u64> {
    data_size
        .checked_add(checkpoints_size)?
        .checked_add(object_links_size)
}
