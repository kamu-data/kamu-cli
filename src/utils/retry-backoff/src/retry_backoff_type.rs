// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How the delay before a retry grows with each attempt
#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryBackoffType {
    /// Always the minimal delay
    Fixed,
    /// The minimal delay times the attempt number
    Linear,
    /// The minimal delay, doubled with each attempt after the first
    Exponential,
    /// Exponential, plus a random part below the minimal delay
    ExponentialWithJitter,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
