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
}

impl DatasetStatistics {
    pub fn with_increment(&self, increment: &DatasetStatistics) -> Self {
        Self {
            last_pulled: increment.last_pulled.or(self.last_pulled),
            num_records: self.num_records + increment.num_records,
            data_size: self.data_size + increment.data_size,
            checkpoints_size: self.checkpoints_size + increment.checkpoints_size,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
