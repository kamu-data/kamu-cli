// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{DatasetLifecycleMessage, MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE};
use messaging_outbox::MockOutbox;
use mockall::predicate::{eq, function};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn expect_outbox_dataset_created(mock_outbox: &mut MockOutbox, times: usize) {
    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
            function(|message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                    Ok(DatasetLifecycleMessage::Created(_))
                )
            }),
        )
        .times(times)
        .returning(|_, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn expect_outbox_dataset_dependencies_updated(
    mock_outbox: &mut MockOutbox,
    times: usize,
) {
    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
            function(|message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                    Ok(DatasetLifecycleMessage::DependenciesUpdated(_))
                )
            }),
        )
        .times(times)
        .returning(|_, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn expect_outbox_dataset_renamed(mock_outbox: &mut MockOutbox, times: usize) {
    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
            function(|message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                    Ok(DatasetLifecycleMessage::Renamed(_))
                )
            }),
        )
        .times(times)
        .returning(|_, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn expect_outbox_dataset_deleted(mock_outbox: &mut MockOutbox, times: usize) {
    mock_outbox
        .expect_post_message_as_json()
        .with(
            eq(MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE),
            function(|message_as_json: &serde_json::Value| {
                matches!(
                    serde_json::from_value::<DatasetLifecycleMessage>(message_as_json.clone()),
                    Ok(DatasetLifecycleMessage::Deleted(_))
                )
            }),
        )
        .times(times)
        .returning(|_, _| Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
