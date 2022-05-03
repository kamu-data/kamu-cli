// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::{DatasetID, DatasetName};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use serde_with::skip_serializing_none;

// TODO: Summary should carry pointers to all refs
// and specify values that change between refs per each "branch"
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetRepositoryInfo {
    pub datasets: Vec<DatasetEntry>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct DatasetEntry {
    pub id: DatasetID,
    pub name: DatasetName,
}
