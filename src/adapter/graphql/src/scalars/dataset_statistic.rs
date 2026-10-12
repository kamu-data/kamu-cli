// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_datasets::TotalStatistic;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Storage usage summed over a set of datasets
#[derive(SimpleObject, Debug)]
pub struct TotalDatasetsStatistic {
    /// Number of data records
    pub total_records: u64,
    /// Size of data files in bytes
    pub total_data_size_bytes: u64,
    /// Size of checkpoint files in bytes
    pub total_checkpoints_size_bytes: u64,
    /// Number of linked objects
    pub total_num_linked_objects: u64,
    /// Size of linked objects in bytes
    pub total_linked_objects_size_bytes: u64,
    /// Size of data, checkpoints and linked objects together in bytes
    pub total_size_bytes: u64,
}

impl From<TotalStatistic> for TotalDatasetsStatistic {
    fn from(value: TotalStatistic) -> Self {
        Self {
            total_records: value.num_records,
            total_data_size_bytes: value.data_size_bytes,
            total_checkpoints_size_bytes: value.checkpoints_size_bytes,
            total_num_linked_objects: value.num_object_links,
            total_linked_objects_size_bytes: value.object_links_size_bytes,
            total_size_bytes: value.get_size_summary(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
