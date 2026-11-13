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

/// The dependency edges every `InMemoryDatasetDependencyRepository` reads and
/// writes, keyed by (downstream, upstream) dataset IDs
pub struct InMemoryDatasetDependencyState {
    pub(crate) edges: InMemoryTransactionalMap<(odf::DatasetID, odf::DatasetID), ()>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetDependencyState {
    pub fn new() -> Self {
        Self {
            edges: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetDependencyState, edges);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
