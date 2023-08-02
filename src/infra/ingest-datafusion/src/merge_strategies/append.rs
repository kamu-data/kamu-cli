// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datafusion::prelude::DataFrame;

use crate::*;

/// Append merge strategy.
///
/// See [opendatafabric::MergeStrategy] for details.
pub struct MergeStrategyAppend;

impl MergeStrategy for MergeStrategyAppend {
    fn merge(&self, _prev: DataFrame, new: DataFrame) -> Result<DataFrame, MergeError> {
        Ok(new)
    }
}
