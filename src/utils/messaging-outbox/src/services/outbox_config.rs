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
    // Each batch is one transaction, which on Sqlite holds the pool's only
    // connection: small batches keep API requests from waiting behind a long
    // catch-up
    pub fn local_default() -> Self {
        Self {
            batch_size: 20,
            consumer_concurrency: Self::DEFAULT_CONSUMER_CONCURRENCY,
        }
    }

    // Postgres pools connections, so larger batches mostly save round trips
    // when catching up on a backlog, e.g. after a restart
    pub fn production_default() -> Self {
        Self {
            batch_size: 100,
            consumer_concurrency: Self::DEFAULT_CONSUMER_CONCURRENCY,
        }
    }

    const DEFAULT_CONSUMER_CONCURRENCY: NonZeroUsize = NonZeroUsize::new(8).unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
