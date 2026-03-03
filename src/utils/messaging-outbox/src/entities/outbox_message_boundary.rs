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
        // Order (tx_id, message_id) is critical here.
        // We must write the highest message_id for the highest tx_id to ensure
        // idempotency. This means there might be messages with higher message_id
        // but lower tx_id in this batch.
        //
        // I.e.:
        //   tx-id: 226813, message-id: 7004-7006, 7009-7011, 7013-7018
        //   tx-id: 226814, message-id: 7003
        //   tx-id: 226815, message-id: 7007-7008
        //
        // Event though the highest message-id is 7018, we must record (226815, 7008) as
        // the last projected offset. Recording (226813, 7018) would cause
        // re-processing of messages from tx-id 226814 and 226815!!!
        //
        // Similarly, we need lowest message-id for the lowest tx-id that has not been
        // processed  when reading the set of applied messages

        self.tx_id
            .cmp(&other.tx_id)
            .then_with(|| self.message_id.cmp(&other.message_id))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
