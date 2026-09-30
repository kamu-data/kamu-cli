// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::num::NonZeroUsize;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OutboxAgentConfig {
    pub batch_size: usize,

    /// Consumers handling messages at once, across all producers. Each one
    /// typically holds a pooled connection while it runs
    pub consumer_concurrency: NonZeroUsize,
}

impl OutboxAgentConfig {
    // On Sqlite each batch holds the only connection, so small batches keep API
    // requests from waiting; concurrent consumers would only queue and time out
    pub fn local_default() -> Self {
        Self {
            batch_size: 20,
            consumer_concurrency: NonZeroUsize::MIN,
        }
    }

    // Postgres pools connections: larger batches save round trips on a backlog,
    // and consumers run in parallel, well below the default pool size
    pub fn production_default() -> Self {
        Self {
            batch_size: 100,
            consumer_concurrency: NonZeroUsize::new(8).unwrap(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
