// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::*;
use internal_error::*;

use crate::*;

/// Ledger merge strategy.
///
/// See [opendatafabric::MergeStrategyLedger] for details.
pub struct MergeStrategyLedger {
    primary_key: Vec<String>,
}

impl MergeStrategyLedger {
    pub fn new(primary_key: Vec<String>) -> Self {
        Self { primary_key }
    }

    pub fn from(cfg: opendatafabric::MergeStrategyLedger) -> Self {
        Self::new(cfg.primary_key)
    }
}

impl MergeStrategy for MergeStrategyLedger {
    // TODO: Ideas to add more comparison modes such as:
    // - expecting input to conain a full superset of prev
    //   - otherwise - raise error
    //   - otherwise - emit retractions
    // - expecting input to contain only new events
    // - expecting input to contain new events and a subset of prev events
    //   - dedupe against entire prev
    //   - seen events should be a tail of the prev (less work to dedupe)
    fn merge(&self, prev: DataFrame, new: DataFrame) -> Result<DataFrame, MergeError> {
        let cols: Vec<_> = self.primary_key.iter().map(|s| s.as_str()).collect();
        let res = new
            .join(prev, JoinType::LeftAnti, &cols, &cols, None)
            .int_err()?;
        Ok(res)
    }
}
