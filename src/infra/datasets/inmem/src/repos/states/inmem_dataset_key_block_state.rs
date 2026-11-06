// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::{InMemoryTransactionParticipant, in_memory_transaction_participant};
use dill::*;

use super::DatasetBlocksMap;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The key blocks every `InMemoryDatasetKeyBlockRepository` reads and writes
pub struct InMemoryDatasetKeyBlockState {
    pub(crate) key_blocks: DatasetBlocksMap,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetKeyBlockState {
    pub fn new() -> Self {
        Self {
            key_blocks: DatasetBlocksMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetKeyBlockState, key_blocks);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
