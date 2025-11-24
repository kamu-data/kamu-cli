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
    pub data_size_bytes: u64,
    pub checkpoints_size_bytes: u64,
    pub num_object_links: u64,
    pub object_links_size_bytes: u64,
}

impl DatasetStatistics {
    pub fn get_size_summary(&self) -> u64 {
        calculate_total_size(
            self.data_size_bytes,
            self.checkpoints_size_bytes,
            self.object_links_size_bytes,
        )
    }
}

impl std::ops::Add for DatasetStatistics {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            last_pulled: other.last_pulled.or(self.last_pulled),
            num_records: self.num_records + other.num_records,
            data_size_bytes: self.data_size_bytes + other.data_size_bytes,
            checkpoints_size_bytes: self.checkpoints_size_bytes + other.checkpoints_size_bytes,
            num_object_links: self.num_object_links + other.num_object_links,
            object_links_size_bytes: self.object_links_size_bytes + other.object_links_size_bytes,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub struct TotalStatistic {
    pub num_records: u64,
    pub data_size_bytes: u64,
    pub checkpoints_size_bytes: u64,
    pub num_object_links: u64,
    pub object_links_size_bytes: u64,
}

impl TotalStatistic {
    pub fn get_size_summary(&self) -> u64 {
        calculate_total_size(
            self.data_size_bytes,
            self.checkpoints_size_bytes,
            self.object_links_size_bytes,
        )
    }

    pub fn add_dataset_statistic(&mut self, dataset_statistic: &DatasetStatistics) {
        self.num_records += dataset_statistic.num_records;
        self.data_size_bytes += dataset_statistic.data_size_bytes;
        self.checkpoints_size_bytes += dataset_statistic.checkpoints_size_bytes;
        self.num_object_links += dataset_statistic.num_object_links;
        self.object_links_size_bytes += dataset_statistic.object_links_size_bytes;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn calculate_total_size(
    data_size_bytes: u64,
    checkpoints_size_bytes: u64,
    object_links_size_bytes: u64,
) -> u64 {
    data_size_bytes
        .wrapping_add(checkpoints_size_bytes)
        .wrapping_add(object_links_size_bytes)
}
