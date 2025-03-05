// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, PartialEq, Eq)]
pub struct DatasetState {
    /// Globally unique identity of the dataset
    pub id: DatasetID<'static>,

    /// Alias to be used in the query
    pub alias: String,

    /// Last block hash of the input datasets that was or should be considered
    /// during the query planning
    pub block_hash: Option<Multihash<'static>>,
}

impl DatasetState {
    pub fn from_query_state(state: kamu_core::QueryState) -> Vec<DatasetState> {
        state
            .input_datasets
            .into_iter()
            .map(
                |(id, kamu_core::QueryStateDataset { alias, block_hash })| DatasetState {
                    id: id.into(),
                    alias,
                    block_hash: Some(block_hash.into()),
                },
            )
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
