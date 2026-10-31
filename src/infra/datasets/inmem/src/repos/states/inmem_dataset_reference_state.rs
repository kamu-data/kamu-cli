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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The references every `InMemoryDatasetReferenceRepository` reads and writes
pub struct InMemoryDatasetReferenceState {
    pub(crate) references:
        InMemoryTransactionalMap<(odf::DatasetID, odf::BlockRef), odf::Multihash>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetReferenceState {
    pub fn new() -> Self {
        Self {
            references: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetReferenceState, references);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
