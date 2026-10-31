// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::sync::{Arc, Mutex, Weak};

use internal_error::{InternalError, ResultIntoInternal};
use sqlx::SqlitePool;

use crate::{
    DatabaseTransactionManager,
    InMemoryTransactionParticipant,
    TransactionId,
    TransactionRef,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Emulates transactions over in-memory state: each transaction collects its
/// changes in the working sets of the registered participants, and its commit
/// applies them in all participants or, on a conflict, in none.
///
/// Opt-in: with `NoOpTransactionManager`, in-memory changes apply at once
pub struct InMemoryTransactionManager {
    // The pool is never connected; `TransactionRef` only needs one to exist
    never_connected_pool: SqlitePool,
    participants: Vec<Arc<dyn InMemoryTransactionParticipant>>,
    open_transactions: Mutex<Vec<OpenTransaction>>,
}

/// A transaction that has not ended yet, and whether it can still end
struct OpenTransaction {
    id: TransactionId,
    liveness: Weak<dyn Any + Send + Sync>,
}

#[dill::component(pub)]
#[dill::interface(dyn DatabaseTransactionManager)]
#[dill::scope(dill::Singleton)]
impl InMemoryTransactionManager {
    pub fn new(participants: Vec<Arc<dyn InMemoryTransactionParticipant>>) -> Self {
        Self {
            never_connected_pool: SqlitePool::connect_lazy("").unwrap(),
            participants,
            open_transactions: Mutex::new(Vec::new()),
        }
    }

    fn rollback_in_participants(&self, transaction_id: TransactionId) {
        for participant in &self.participants {
            participant.rollback(transaction_id);
        }
    }

    fn end(&self, transaction_id: TransactionId) {
        self.open_transactions
            .lock()
            .unwrap()
            .retain(|open_transaction| open_transaction.id != transaction_id);
    }

    /// Drops the working sets of transactions whose references are all gone
    /// without a commit or rollback, as after a panic
    fn drop_abandoned_transactions(&self) {
        let abandoned: Vec<TransactionId> = {
            let mut open_transactions = self.open_transactions.lock().unwrap();
            let (abandoned, alive): (Vec<_>, Vec<_>) = open_transactions
                .drain(..)
                .partition(|open_transaction| open_transaction.liveness.strong_count() == 0);
            *open_transactions = alive;
            abandoned
                .into_iter()
                .map(|open_transaction| open_transaction.id)
                .collect()
        };
        for transaction_id in abandoned {
            self.rollback_in_participants(transaction_id);
        }
    }
}

#[async_trait::async_trait]
impl DatabaseTransactionManager for InMemoryTransactionManager {
    async fn make_transaction_ref(&self) -> Result<TransactionRef, InternalError> {
        self.drop_abandoned_transactions();

        let transaction_ref = TransactionRef::new(self.never_connected_pool.clone());
        for participant in &self.participants {
            participant.begin(transaction_ref.id());
        }
        self.open_transactions
            .lock()
            .unwrap()
            .push(OpenTransaction {
                id: transaction_ref.id(),
                liveness: transaction_ref.liveness(),
            });
        Ok(transaction_ref)
    }

    async fn commit_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_id = transaction_ref.id();
        self.end(transaction_id);

        // Every participant stays locked from its check until all changes apply,
        // so no other change lands in between. Participants lock in a fixed order
        let prepared = self
            .participants
            .iter()
            .map(|participant| participant.prepare_commit(transaction_id))
            .collect::<Result<Vec<_>, _>>();
        match prepared {
            Ok(prepared) => {
                for prepared_commit in prepared {
                    prepared_commit.apply();
                }
                Ok(())
            }
            Err(conflict) => {
                self.rollback_in_participants(transaction_id);
                Err(conflict).int_err()
            }
        }
    }

    async fn rollback_transaction(
        &self,
        transaction_ref: TransactionRef,
    ) -> Result<(), InternalError> {
        let transaction_id = transaction_ref.id();
        self.end(transaction_id);
        self.rollback_in_participants(transaction_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
mod tests {
    use super::*;
    use crate::InMemoryTransactionalMap;

    #[tokio::test]
    async fn test_abandoned_transaction_changes_are_dropped() {
        let harness = ManagerHarness::new();
        let abandoned = harness.manager.make_transaction_ref().await.unwrap();
        let abandoned_id = abandoned.id();
        harness
            .map
            .write(Some(abandoned_id), |write| write.put("a", 1));
        drop(abandoned);

        // The next transaction drops what the abandoned one left
        let _next = harness.manager.make_transaction_ref().await.unwrap();
        harness.map.prepare_commit(abandoned_id).unwrap().apply();

        pretty_assertions::assert_eq!(harness.map.read(None, |view| view.get(&"a").copied()), None);
    }

    #[tokio::test]
    async fn test_commit_applies_and_rollback_drops() {
        let harness = ManagerHarness::new();
        let committed = harness.manager.make_transaction_ref().await.unwrap();
        harness
            .map
            .write(Some(committed.id()), |write| write.put("a", 1));
        let rolled_back = harness.manager.make_transaction_ref().await.unwrap();
        harness
            .map
            .write(Some(rolled_back.id()), |write| write.put("b", 2));

        harness.manager.commit_transaction(committed).await.unwrap();
        harness
            .manager
            .rollback_transaction(rolled_back)
            .await
            .unwrap();

        pretty_assertions::assert_eq!(
            harness.map.read(None, |view| (
                view.get(&"a").copied(),
                view.get(&"b").copied()
            )),
            (Some(1), None)
        );
    }

    struct ManagerHarness {
        map: Arc<InMemoryTransactionalMap<&'static str, i32>>,
        manager: InMemoryTransactionManager,
    }

    impl ManagerHarness {
        fn new() -> Self {
            let map = Arc::new(InMemoryTransactionalMap::new());
            let manager = InMemoryTransactionManager::new(vec![map.clone()]);
            Self { map, manager }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
