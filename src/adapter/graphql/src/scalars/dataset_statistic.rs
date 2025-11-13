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

#[derive(SimpleObject, Debug)]
pub struct TotalDatasetsStatistic {
    pub num_records: u64,
    pub data_size: u64,
    pub checkpoints_size: u64,
    pub num_object_links: u64,
    pub object_links_size: u64,
    pub total_size: Option<u64>,
}

impl From<TotalStatistic> for TotalDatasetsStatistic {
    fn from(value: TotalStatistic) -> Self {
        Self {
            num_records: value.num_records,
            data_size: value.data_size,
            checkpoints_size: value.checkpoints_size,
            num_object_links: value.num_object_links,
            object_links_size: value.object_links_size,
            total_size: value.get_size_summary(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
