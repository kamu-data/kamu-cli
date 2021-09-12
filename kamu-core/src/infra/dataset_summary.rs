// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::serde::yaml::formats::datetime_rfc3339_opt;
use opendatafabric::{DatasetIDBuf, Sha3_256};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum DatasetKind {
    Root,
    Derivative,
}

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSummary {
    pub id: DatasetIDBuf,
    pub kind: DatasetKind,
    pub last_block_hash: Sha3_256,
    pub dependencies: Vec<DatasetIDBuf>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub last_pulled: Option<DateTime<Utc>>,
    pub num_records: u64,
    pub data_size: u64,
}

impl DatasetSummary {
    pub fn new(id: DatasetIDBuf) -> Self {
        Self {
            id,
            kind: DatasetKind::Root,
            last_block_hash: Sha3_256::zero(),
            dependencies: Vec::new(),
            last_pulled: None,
            num_records: 0,
            data_size: 0,
        }
    }
}
