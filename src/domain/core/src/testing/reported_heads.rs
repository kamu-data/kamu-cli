// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Old and new `HEAD` that a mocked operation reports, set by the test once the
/// blocks exist, after the mock has been registered.
#[derive(Debug, Clone, Default)]
pub struct ReportedHeads(Arc<Mutex<Option<(odf::Multihash, odf::Multihash)>>>);

impl ReportedHeads {
    pub fn set(&self, old_head: &odf::Multihash, new_head: &odf::Multihash) {
        *self.0.lock().unwrap() = Some((old_head.clone(), new_head.clone()));
    }

    pub fn get(&self) -> (odf::Multihash, odf::Multihash) {
        self.0
            .lock()
            .unwrap()
            .clone()
            .expect("Reported heads must be set before the mocked operation runs")
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
