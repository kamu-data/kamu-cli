// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use opendatafabric::serde::yaml::*;
use opendatafabric::{DatasetID, DatasetKind, Multihash, TransformInput};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::skip_serializing_none;

// TODO: Summary should carry pointers to all refs
// and specify values that change between refs per each "branch"
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetSummary {
    pub id: DatasetID,
    #[serde_as(as = "DatasetKindDef")]
    pub kind: DatasetKind,
    pub last_block_hash: Multihash,
    #[serde_as(as = "Vec<TransformInputDef>")]
    pub dependencies: Vec<TransformInput>,
    #[serde(default, with = "datetime_rfc3339_opt")]
    pub last_pulled: Option<DateTime<Utc>>,
    pub num_records: u64,
    pub data_size: u64,
    pub checkpoints_size: u64,   
}

impl DatasetSummary {
    
    pub fn default_summary(last_block_hash: &Multihash) -> Self {
        Self {
            id: DatasetID::from_pub_key_ed25519(b""), // Will be replaced
            kind: DatasetKind::Root,
            last_block_hash: last_block_hash.clone(),
            dependencies: Vec::new(),
            last_pulled: None,
            num_records: 0,
            data_size: 0,
            checkpoints_size: 0,
        }
    }
}
