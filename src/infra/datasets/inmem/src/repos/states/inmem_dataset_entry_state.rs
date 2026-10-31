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
use kamu_datasets::DatasetEntry;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The entries every `InMemoryDatasetEntryRepository` reads and writes
pub struct InMemoryDatasetEntryState {
    pub(crate) rows: InMemoryTransactionalMap<odf::DatasetID, DatasetEntry>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetEntryState {
    pub fn new() -> Self {
        Self {
            rows: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetEntryState, rows);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
