// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::Utc;
use dill::Component;
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccountRepository, InMemoryDidSecretKeyRepository};
use kamu_accounts_services::utils::AccountAuthorizationHelperImpl;
use kamu_accounts_services::*;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{EsTestContext, SearchTestResponse};
use kamu_search_services::{SearchIndexer, SearchServiceImpl};
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_account_index_initially_empty(ctx: Arc<EsTestContext>) {
    let harness = AccountIndexingHarness::new(ctx).await;

    let accounts_index_response = harness.view_accounts_index().await;
    assert_eq!(accounts_index_response.total_hits(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_accounts_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = AccountIndexingHarness::new(ctx).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(account_name).await;
    }

    let accounts_index_response = harness.view_accounts_index().await;

    assert_eq!(accounts_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        account_names
            .iter()
            .map(|name| harness.account_id_from_name(name).to_string())
            .collect::<Vec<_>>()
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alice",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alice",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "bob",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "bob",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_updating_account_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = AccountIndexingHarness::new(ctx).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(account_name).await;
    }

    harness.rename_account("bob", "robert").await;
    harness.rename_account("alice", "alicia").await;

    let accounts_index_response = harness.view_accounts_index().await;

    assert_eq!(accounts_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        vec![
            // Althouhgh renamed, IDs remain the same.
            // However, the order changes, as we sort by account_name
            harness.account_id_from_name("alice").to_string(),
            harness.account_id_from_name("charlie").to_string(),
            harness.account_id_from_name("bob").to_string(),
        ]
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alicia",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alicia",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "robert",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "robert",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_deleting_account_reflected_in_index(ctx: Arc<EsTestContext>) {
    let harness = AccountIndexingHarness::new(ctx).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(account_name).await;
    }

    harness.delete_account("bob").await;

    let accounts_index_response = harness.view_accounts_index().await;
    assert_eq!(accounts_index_response.total_hits(), 2);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        vec![
            harness.account_id_from_name("alice").to_string(),
            harness.account_id_from_name("charlie").to_string(),
        ]
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alice",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alice",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct AccountIndexingHarness {
    fixed_time: chrono::DateTime<Utc>,
    ctx: Arc<EsTestContext>,
    catalog: dill::Catalog,
}

impl AccountIndexingHarness {
    pub async fn new(ctx: Arc<EsTestContext>) -> Self {
        let mut b = dill::CatalogBuilder::new_chained(ctx.catalog());
        b.add::<CreateAccountUseCaseImpl>()
            .add::<UpdateAccountUseCaseImpl>()
            .add::<DeleteAccountUseCaseImpl>()
            .add::<InMemoryAccountRepository>()
            .add::<InMemoryDidSecretKeyRepository>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add::<AccountServiceImpl>()
            .add::<AccountAuthorizationHelperImpl>()
            .add::<kamu_auth_rebac_services::RebacServiceImpl>()
            .add::<kamu_auth_rebac_inmem::InMemoryRebacRepository>()
            .add_value(kamu_auth_rebac_services::DefaultAccountProperties::default())
            .add_value(kamu_auth_rebac_services::DefaultDatasetProperties::default())
            .add::<AccountSearchSchemaProvider>()
            .add::<AccountSearchUpdater>()
            .add::<SearchIndexer>()
            .add::<SearchServiceImpl>()
            .add_builder(
                messaging_outbox::OutboxImmediateImpl::builder()
                    .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
            )
            .bind::<dyn Outbox, OutboxImmediateImpl>()
            .add::<SystemTimeSourceStub>();

        register_message_dispatcher::<AccountLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        );

        let catalog = b.build();

        let fixed_time = Utc::now();
        let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();
        time_source.set(fixed_time);

        use init_on_startup::InitOnStartup;
        let indexer = catalog.get_one::<SearchIndexer>().unwrap();
        indexer.run_initialization().await.unwrap();

        Self {
            fixed_time,
            ctx,
            catalog,
        }
    }

    pub async fn create_account(&self, account_name: &str) {
        let account = Account {
            registered_at: self.fixed_time,
            ..Account::test(self.account_id_from_name(account_name), account_name)
        };

        let create_account_uc = self.catalog.get_one::<dyn CreateAccountUseCase>().unwrap();
        create_account_uc
            .execute(&account, &TEST_PASSWORD, false /* quiet */)
            .await
            .unwrap();
    }

    pub async fn rename_account(&self, old_name: &str, new_name: &str) {
        // Locate account
        let account_svc = self.catalog.get_one::<dyn AccountService>().unwrap();
        let account = account_svc
            .account_by_name(&odf::AccountName::new_unchecked(old_name))
            .await
            .unwrap()
            .unwrap();

        // Prepare updated account
        let mut updated_account = account.clone();
        updated_account.account_name = odf::AccountName::new_unchecked(new_name);
        updated_account.display_name = new_name.to_string();

        // Execute update on user's behalf in authenticated context
        {
            let mut b = dill::CatalogBuilder::new_chained(&self.catalog);
            b.add_value(CurrentAccountSubject::logged(
                account.id.clone(),
                account.account_name.clone(),
            ));
            let authenticated_catalog = b.build();

            let update_account_uc = authenticated_catalog
                .get_one::<dyn UpdateAccountUseCase>()
                .unwrap();
            update_account_uc.execute(&updated_account).await.unwrap();
        }
    }

    pub async fn delete_account(&self, account_name: &str) {
        // Locate account
        let account_svc = self.catalog.get_one::<dyn AccountService>().unwrap();
        let account = account_svc
            .account_by_name(&odf::AccountName::new_unchecked(account_name))
            .await
            .unwrap()
            .unwrap();

        // Execute delete on user's behalf in authenticated context
        {
            let mut b = dill::CatalogBuilder::new_chained(&self.catalog);
            b.add_value(CurrentAccountSubject::logged(
                account.id.clone(),
                account.account_name.clone(),
            ));
            let authenticated_catalog = b.build();

            let delete_account_uc = authenticated_catalog
                .get_one::<dyn DeleteAccountUseCase>()
                .unwrap();
            delete_account_uc.execute(&account).await.unwrap();
        }
    }

    pub fn account_id_from_name(&self, account_name: &str) -> odf::AccountID {
        odf::AccountID::new_seeded_ed25519(account_name.as_bytes())
    }

    pub async fn view_accounts_index(&self) -> SearchTestResponse {
        self.ctx.refresh_indices().await;

        let search_repo = self.ctx.search_repo();

        let seach_response = search_repo
            .search(SearchRequest {
                query: None,
                entity_schemas: vec![account_search_schema::SCHEMA_NAME],
                source: SearchRequestSourceSpec::All,
                filter: None,
                sort: sort!(account_search_schema::fields::ACCOUNT_NAME),
                page: SearchPaginationSpec {
                    limit: 100,
                    offset: 0,
                },
                options: SearchOptions::default(),
            })
            .await
            .unwrap();

        SearchTestResponse(seach_response)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
