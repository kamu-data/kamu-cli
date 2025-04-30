// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{Catalog, CatalogBuilder};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_accounts::{
    AccessTokenLifecycleMessage,
    AccessTokenService,
    Account,
    AccountRepository,
    MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
};
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::AccessTokenServiceImpl;
use messaging_outbox::{MockOutbox, Outbox};
use time_source::{SystemTimeSource, SystemTimeSourceStub};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_create_access_token() {
    let mut mock_outbox = MockOutbox::new();
    AccessTokenServiceHarness::expect_outbox_access_token_created(&mut mock_outbox);

    let harness = AccessTokenServiceHarness::new(mock_outbox);
    let account = Account::dummy();
    harness.save_account(&account).await.unwrap();
    let access_token = harness
        .access_token_service
        .create_access_token("foo", &account.id)
        .await;
    assert!(access_token.is_ok());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AccessTokenServiceHarness {
    _catalog: Catalog,
    access_token_service: Arc<dyn AccessTokenService>,
    account_repo: Arc<dyn AccountRepository>,
}

impl AccessTokenServiceHarness {
    fn new(mock_outbox: MockOutbox) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();
            b.add_value(mock_outbox);
            b.add::<InMemoryAccessTokenRepository>();
            b.add::<AccessTokenServiceImpl>();
            b.add_value(SystemTimeSourceStub::new());
            b.bind::<dyn SystemTimeSource, SystemTimeSourceStub>();
            b.add::<InMemoryAccountRepository>();
            b.bind::<dyn Outbox, MockOutbox>();
            b.build()
        };
        Self {
            access_token_service: catalog.get_one().unwrap(),
            account_repo: catalog.get_one().unwrap(),
            _catalog: catalog,
        }
    }

    async fn save_account(&self, account: &Account) -> Result<(), InternalError> {
        self.account_repo.save_account(account).await.int_err()
    }

    fn expect_outbox_access_token_created(mock_outbox: &mut MockOutbox) {
        use mockall::predicate::{always, eq, function};
        mock_outbox
            .expect_post_message_as_json()
            .with(
                eq(MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE),
                function(|message_as_json: &serde_json::Value| {
                    matches!(
                        serde_json::from_value::<AccessTokenLifecycleMessage>(
                            message_as_json.clone()
                        ),
                        Ok(AccessTokenLifecycleMessage::Created(_))
                    )
                }),
                always(),
            )
            .returning(|_, _, _| Ok(()));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
