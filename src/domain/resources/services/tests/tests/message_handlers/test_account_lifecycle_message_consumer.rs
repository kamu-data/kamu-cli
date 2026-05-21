// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::Utc;
use dill::CatalogBuilder;
use email_utils::Email;
use internal_error::InternalError;
use kamu_accounts::{AccountLifecycleMessage, MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE};
use kamu_resources::{DeleteAccountResourcesUseCase, MockDeleteAccountResourcesUseCase};
use messaging_outbox::{OutboxProvider, register_message_dispatcher};

use crate::tests::utils::make_account_id;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_deleted_message_triggers_cascade_delete() {
    let account_id = make_account_id();
    let account_id_clone = account_id.clone();

    let harness = AccountLifecycleConsumerHarness::new(
        AccountLifecycleConsumerHarness::expect_execute_once(account_id_clone),
    );

    let message = AccountLifecycleMessage::deleted(
        Utc::now(),
        account_id,
        Email::parse("test@example.com").unwrap(),
        "Test User".to_string(),
    );

    harness.consume_message(&message).await.unwrap();
    // Mock verifies call counts on drop
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_created_message_is_no_op() {
    let harness =
        AccountLifecycleConsumerHarness::new(AccountLifecycleConsumerHarness::expect_no_execute());

    let message = AccountLifecycleMessage::created(
        Utc::now(),
        make_account_id(),
        Email::parse("test@example.com").unwrap(),
        odf::AccountName::new_unchecked("testuser"),
        "Test User".to_string(),
    );

    harness.consume_message(&message).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_updated_message_is_no_op() {
    let harness =
        AccountLifecycleConsumerHarness::new(AccountLifecycleConsumerHarness::expect_no_execute());

    let message = AccountLifecycleMessage::updated(
        Utc::now(),
        make_account_id(),
        Email::parse("old@example.com").unwrap(),
        Email::parse("new@example.com").unwrap(),
        odf::AccountName::new_unchecked("oldname"),
        odf::AccountName::new_unchecked("newname"),
        "Old Name".to_string(),
        "New Name".to_string(),
    );

    harness.consume_message(&message).await.unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AccountLifecycleConsumerHarness {
    catalog: dill::Catalog,
}

impl AccountLifecycleConsumerHarness {
    fn new(mock: MockDeleteAccountResourcesUseCase) -> Self {
        // Minimal catalog: only the consumer and its single dependency (the mock
        // use case). Deliberately not chained from BaseResourceServiceHarness to
        // avoid the ambiguous binding that would arise from the real
        // DeleteAccountResourcesUsecaseImpl registered there.
        let mut b = CatalogBuilder::new();

        OutboxProvider::Immediate {
            force_immediate: true,
        }
        .embed_into_catalog(&mut b);

        b.add_value(mock)
            .bind::<dyn DeleteAccountResourcesUseCase, MockDeleteAccountResourcesUseCase>();

        b.add::<kamu_resources_services::AccountLifecycleMessageConsumer>();

        register_message_dispatcher::<AccountLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        );

        Self { catalog: b.build() }
    }

    fn expect_execute_once(account_id: odf::AccountID) -> MockDeleteAccountResourcesUseCase {
        let mut mock = MockDeleteAccountResourcesUseCase::new();
        mock.expect_execute()
            .once()
            .withf(move |id| id == &account_id)
            .returning(|_| Ok(()));
        mock
    }

    fn expect_no_execute() -> MockDeleteAccountResourcesUseCase {
        let mut mock = MockDeleteAccountResourcesUseCase::new();
        mock.expect_execute().never();
        mock
    }

    async fn consume_message(
        &self,
        message: &AccountLifecycleMessage,
    ) -> Result<(), InternalError> {
        use messaging_outbox::MessageConsumerT;

        self.catalog
            .get_one::<dyn MessageConsumerT<AccountLifecycleMessage>>()
            .unwrap()
            .consume_message(&self.catalog, message)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
