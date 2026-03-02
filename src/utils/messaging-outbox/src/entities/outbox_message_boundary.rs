// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::OutboxMessageID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct OutboxMessageBoundary {
    pub message_id: OutboxMessageID,
    pub tx_id: i64,
}

impl Default for OutboxMessageBoundary {
    fn default() -> Self {
        Self {
            message_id: OutboxMessageID::new(0),
            tx_id: 0,
        }
    }
}

impl PartialOrd for OutboxMessageBoundary {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OutboxMessageBoundary {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.tx_id
            .cmp(&other.tx_id)
            .then_with(|| self.message_id.cmp(&other.message_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
