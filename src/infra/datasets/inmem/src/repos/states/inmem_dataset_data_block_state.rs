// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{
    InMemoryTransactionParticipant,
    InMemoryTransactionalMap,
    in_memory_transaction_participant,
};
use dill::*;
use kamu_datasets::DatasetBlock;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type DataBlocks = InMemoryTransactionalMap<(odf::DatasetID, odf::BlockRef), Vec<DatasetBlock>>;

/// The data blocks every `InMemoryDatasetDataBlockRepository` reads and writes
pub struct InMemoryDatasetDataBlockState {
    pub(crate) data_blocks: DataBlocks,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetDataBlockState {
    pub fn new() -> Self {
        Self {
            data_blocks: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetDataBlockState, data_blocks);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
