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
use kamu_datasets::DatasetStatistics;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The statistics every `InMemoryDatasetStatisticsRepository` reads and writes
pub struct InMemoryDatasetStatisticsState {
    pub(crate) statistics:
        InMemoryTransactionalMap<(odf::DatasetID, odf::BlockRef), DatasetStatistics>,
}

#[component(pub)]
#[interface(dyn InMemoryTransactionParticipant)]
#[scope(Singleton)]
impl InMemoryDatasetStatisticsState {
    pub fn new() -> Self {
        Self {
            statistics: InMemoryTransactionalMap::new(),
        }
    }
}

in_memory_transaction_participant!(InMemoryDatasetStatisticsState, statistics);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
