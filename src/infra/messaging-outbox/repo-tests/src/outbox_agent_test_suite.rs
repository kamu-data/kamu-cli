// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The agent over messages held back by an older open transaction. The
//! fixtures open their own transactions

use std::time::Duration;

use dill::Catalog;

use crate::OutboxAgentHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_drain_waits_for_messages_held_back_by_older_transaction(catalog: &Catalog) {
    let harness = OutboxAgentHarness::new(catalog).await;

    // The older transaction writes first, so its message sorts below the newer
    // one's
    let older_tx = harness.begin().await;
    harness.push_message(&older_tx, 1).await;

    let newer_tx = harness.begin().await;
    harness.push_message(&newer_tx, 2).await;
    harness.commit(newer_tx).await;

    // Message 2 is committed but held back: delivering it now would let the
    // boundary pass message 1
    let drain = harness.spawn_drain();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(!drain.is_finished());

    harness.commit(older_tx).await;
    drain.await.unwrap();

    assert_eq!(harness.consumed_seq(), [1, 2]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_drain_ignores_held_back_messages_nobody_consumes(catalog: &Catalog) {
    let harness = OutboxAgentHarness::new(catalog).await;

    let older_tx = harness.begin_older_transaction().await;

    // Held back, but no consumer would ever take it
    let newer_tx = harness.begin().await;
    harness.push_unconsumed_message(&newer_tx).await;
    harness.commit(newer_tx).await;

    tokio::time::timeout(Duration::from_secs(2), harness.spawn_drain())
        .await
        .expect("drain must not wait for the older transaction")
        .unwrap();

    harness.commit(older_tx).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_drain_ignores_held_back_messages_of_failed_consumers(catalog: &Catalog) {
    let harness = OutboxAgentHarness::new(catalog).await;

    // The only consumer of this producer fails, and the agent stops feeding it
    let first_tx = harness.begin().await;
    harness.push_failing_message(&first_tx).await;
    harness.commit(first_tx).await;
    harness.drain().await;

    let older_tx = harness.begin_older_transaction().await;

    // Held back, but this agent would not deliver it
    let newer_tx = harness.begin().await;
    harness.push_failing_message(&newer_tx).await;
    harness.commit(newer_tx).await;

    tokio::time::timeout(Duration::from_secs(2), harness.spawn_drain())
        .await
        .expect("drain must not wait for the older transaction")
        .unwrap();

    harness.commit(older_tx).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
