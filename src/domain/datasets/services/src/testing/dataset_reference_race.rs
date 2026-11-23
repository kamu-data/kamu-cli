// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;

use super::ConcurrentReferenceMove;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a racing dataset does before its next reference write, after the
/// writer read the head it expects: a concurrent writer moves or removes the
/// reference, or the storage refuses the write
#[derive(Default)]
pub struct DatasetReferenceRace {
    armed: Mutex<Option<ArmedRace>>,
}

enum ArmedRace {
    ConcurrentMove(ConcurrentReferenceMove),
    Deny,
}

impl DatasetReferenceRace {
    /// Arms the next reference write to move the reference to
    /// `concurrent_block_hash` first, in a transaction of its own made from
    /// `catalog`
    pub fn move_reference_before_next_update(
        &self,
        catalog: &dill::Catalog,
        concurrent_block_hash: odf::Multihash,
    ) {
        self.arm(ArmedRace::ConcurrentMove(ConcurrentReferenceMove::new(
            catalog,
            concurrent_block_hash,
        )));
    }

    /// Arms the next reference write to remove the reference first, in a
    /// transaction of its own made from `catalog`
    pub fn remove_reference_before_next_update(&self, catalog: &dill::Catalog) {
        self.arm(ArmedRace::ConcurrentMove(
            ConcurrentReferenceMove::removing(catalog),
        ));
    }

    /// Arms the next reference write to fail as not allowed, moving nothing,
    /// as a read-only storage does
    pub fn deny_next_update(&self) {
        self.arm(ArmedRace::Deny);
    }

    fn arm(&self, race: ArmedRace) {
        *self.armed.lock().unwrap() = Some(race);
    }

    /// Runs what is armed, once
    pub(crate) async fn before_update(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let armed = self.armed.lock().unwrap().take();
        match armed {
            None => {}
            Some(ArmedRace::ConcurrentMove(concurrent_reference_move)) => {
                concurrent_reference_move.apply(dataset_id, block_ref).await;
            }
            Some(ArmedRace::Deny) => {
                return Err(odf::dataset::SetChainRefError::Access(
                    odf::AccessError::ReadOnly(None),
                ));
            }
        }
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
