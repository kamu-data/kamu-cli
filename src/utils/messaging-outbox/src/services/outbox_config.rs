// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct OutboxConfig {
    /// Defines discretion for main scheduling loop: how often new data is
    /// checked and processed
    pub awaiting_step: Duration,
    /// Defines maximum number of messages attempted to read in 1 step
    pub batch_size: i64,
    /// Minimal relevance level
    pub minimal_relevance: MessageRelevance,
}

impl OutboxConfig {
    pub fn new(
        awaiting_step: Duration,
        batch_size: i64,
        minimal_relevance: MessageRelevance,
    ) -> Self {
        Self {
            awaiting_step,
            batch_size,
            minimal_relevance,
        }
    }

    pub fn for_tests() -> Self {
        Self {
            awaiting_step: Duration::seconds(1),
            batch_size: 20,
            minimal_relevance: MessageRelevance::Essential,
        }
    }

    pub fn for_tests_with_diagnostic_on() -> Self {
        Self {
            awaiting_step: Duration::seconds(1),
            batch_size: 20,
            minimal_relevance: MessageRelevance::Diagnostic,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessageRelevance {
    Essential = 50,
    Diagnostic = 10,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
