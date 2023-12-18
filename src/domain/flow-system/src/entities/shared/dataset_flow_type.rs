// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq)]
pub enum DatasetFlowType {
    Ingest,
    ExecuteQuery,
    Compaction,
}

impl DatasetFlowType {
    pub fn all() -> &'static [DatasetFlowType] {
        &[Self::Ingest, Self::ExecuteQuery, Self::Compaction]
    }

    pub fn is_dataset_update(&self) -> bool {
        *self == DatasetFlowType::Ingest || *self == DatasetFlowType::ExecuteQuery
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
