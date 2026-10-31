// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{InMemoryWriteConflictError, TransactionId};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// In-memory state that keeps the changes of each transaction opened by
/// `InMemoryTransactionManager` apart until that transaction ends
pub trait InMemoryTransactionParticipant: Send + Sync {
    /// Starts collecting the changes of `transaction_id`
    fn begin(&self, transaction_id: TransactionId);

    /// Locks the state and checks that no other transaction committed a change
    /// the working set of `transaction_id` was based on. The state stays
    /// locked until the returned commit is applied or dropped, so nothing can
    /// change in between
    fn prepare_commit(
        &self,
        transaction_id: TransactionId,
    ) -> Result<Box<dyn InMemoryPreparedCommit + '_>, InMemoryWriteConflictError>;

    /// Drops the working set of `transaction_id`
    fn rollback(&self, transaction_id: TransactionId);
}

/// A checked commit of one participant, holding its lock
pub trait InMemoryPreparedCommit {
    /// Applies the working set and releases the lock
    fn apply(self: Box<Self>);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Implements [`InMemoryTransactionParticipant`] for a state type by
/// forwarding to its [`InMemoryTransactionalMap`](crate::InMemoryTransactionalMap)
/// field
#[macro_export]
macro_rules! in_memory_transaction_participant {
    ($state:ty, $map:ident) => {
        impl $crate::InMemoryTransactionParticipant for $state {
            fn begin(&self, transaction_id: $crate::TransactionId) {
                $crate::InMemoryTransactionParticipant::begin(&self.$map, transaction_id);
            }

            fn prepare_commit(
                &self,
                transaction_id: $crate::TransactionId,
            ) -> Result<
                Box<dyn $crate::InMemoryPreparedCommit + '_>,
                $crate::InMemoryWriteConflictError,
            > {
                $crate::InMemoryTransactionParticipant::prepare_commit(&self.$map, transaction_id)
            }

            fn rollback(&self, transaction_id: $crate::TransactionId) {
                $crate::InMemoryTransactionParticipant::rollback(&self.$map, transaction_id);
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
