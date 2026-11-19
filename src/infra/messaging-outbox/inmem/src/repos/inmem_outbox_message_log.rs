// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use database_common::TransactionId;
use itertools::Itertools as _;
use messaging_outbox::{NewOutboxMessage, OutboxMessage, OutboxMessageID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Outbox messages as a database keeps them: an ID is taken from a sequence
/// when a message is pushed, and the message becomes visible to other
/// transactions when its transaction commits. A rolled-back transaction
/// leaves a gap in the IDs.
///
/// Like the Postgres bridge, readers see committed messages only below the
/// first ID still held by another open transaction. A message committed later
/// with a lower ID would otherwise sort below a boundary already recorded and
/// never be read.
#[derive(Default)]
pub(crate) struct OutboxMessageLog {
    /// Ordered by ID
    committed: Vec<OutboxMessage>,
    /// The messages of each open transaction, ordered by ID
    pending: HashMap<TransactionId, Vec<OutboxMessage>>,
    last_message_id: i64,
}

impl OutboxMessageLog {
    pub(crate) fn begin(&mut self, transaction_id: TransactionId) {
        self.pending.insert(transaction_id, Vec::new());
    }

    /// Appends the message to the open transaction, or commits it at once
    /// when no tracked transaction is given. Returns whether it was committed
    pub(crate) fn push(
        &mut self,
        transaction_id: Option<TransactionId>,
        new_message: &NewOutboxMessage,
    ) -> bool {
        self.last_message_id += 1;
        let message = new_message.as_outbox_message(OutboxMessageID::new(self.last_message_id), 0);

        match transaction_id.and_then(|transaction_id| self.pending.get_mut(&transaction_id)) {
            Some(pending) => {
                pending.push(message);
                false
            }
            None => {
                self.committed.push(message);
                true
            }
        }
    }

    /// Makes the messages of the transaction visible. Returns whether it had
    /// any
    pub(crate) fn commit(&mut self, transaction_id: TransactionId) -> bool {
        let Some(messages) = self.pending.remove(&transaction_id) else {
            return false;
        };
        if messages.is_empty() {
            return false;
        }

        self.committed.extend(messages);
        self.committed.sort_by_key(|message| message.message_id);
        true
    }

    pub(crate) fn rollback(&mut self, transaction_id: TransactionId) {
        self.pending.remove(&transaction_id);
    }

    /// The messages the transaction sees, ordered by ID: committed ones not
    /// held back, and its own
    pub(crate) fn visible(
        &self,
        transaction_id: Option<TransactionId>,
    ) -> impl Iterator<Item = &OutboxMessage> {
        let held_back_from = self.held_back_from(transaction_id);
        let own = transaction_id
            .and_then(|transaction_id| self.pending.get(&transaction_id))
            .into_iter()
            .flatten();

        self.committed
            .iter()
            .take_while(move |message| {
                held_back_from.is_none_or(|held_back_from| message.message_id < held_back_from)
            })
            .merge_by(own, |committed, own| committed.message_id < own.message_id)
    }

    /// Whether committed messages of the producers wait for another open
    /// transaction that holds a lower ID
    pub(crate) fn has_held_back(
        &self,
        transaction_id: Option<TransactionId>,
        producer_names: &[&str],
    ) -> bool {
        let Some(held_back_from) = self.held_back_from(transaction_id) else {
            return false;
        };

        self.committed.iter().any(|message| {
            message.message_id >= held_back_from
                && producer_names.contains(&message.producer_name.as_str())
        })
    }

    /// The lowest ID held by another open transaction
    fn held_back_from(&self, transaction_id: Option<TransactionId>) -> Option<OutboxMessageID> {
        self.pending
            .iter()
            .filter(|(id, _)| Some(**id) != transaction_id)
            .filter_map(|(_, messages)| messages.first())
            .map(|message| message.message_id)
            .min()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
