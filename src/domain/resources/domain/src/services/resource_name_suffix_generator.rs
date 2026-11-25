// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Makes the names generated for resources applied without one unique
pub trait ResourceNameSuffixGenerator: Send + Sync {
    /// Lowercase letters and digits; repeats are possible, if unlikely
    fn generate_suffix(&self) -> String;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
