// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::DatasetRefRemote;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;

#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetConfig {
    pub pull_aliases: Vec<DatasetRefRemote>,
    pub push_aliases: Vec<DatasetRefRemote>,
}

impl Default for DatasetConfig {
    fn default() -> Self {
        Self {
            pull_aliases: Vec::new(),
            push_aliases: Vec::new(),
        }
    }
}
