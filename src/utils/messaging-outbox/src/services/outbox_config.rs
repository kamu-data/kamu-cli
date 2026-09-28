// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxAgentConfig {
    pub batch_size: usize,
}

impl OutboxAgentConfig {
    pub fn local_default() -> Self {
        Self { batch_size: 20 }
    }

    pub fn production_default() -> Self {
        Self { batch_size: 100 }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
