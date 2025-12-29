// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::Component;
use kamu_accounts::*;
use kamu_accounts_services::*;
use kamu_search::*;
use kamu_search_elasticsearch::testing::{
    ElasticsearchBaseHarness,
    ElasticsearchTestContext,
    SearchTestResponse,
};
use messaging_outbox::{Outbox, OutboxImmediateImpl, register_message_dispatcher};

use crate::tests::use_cases::{AccountBaseUseCaseHarness, AccountBaseUseCaseHarnessOpts};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_account_index_initially_empty(ctx: Arc<ElasticsearchTestContext>) {
    let harness = AccountIndexingHarness::new(ctx, None).await;

    let accounts_index_response = harness.view_accounts_index().await;
    assert_eq!(accounts_index_response.total_hits(), 0);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_predefined_accounts_appear_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let registered_at = Utc::now();

    let predefined_account_config = {
        let mut p = PredefinedAccountsConfig::new();
        p.predefined = vec![
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("john-doe"))
                .set_registered_at(registered_at)
                .set_display_name("John Doe".to_string()),
            AccountConfig::test_config_from_name(odf::AccountName::new_unchecked("jane-smith"))
                .set_registered_at(registered_at),
        ];
        p
    };

    let harness = AccountIndexingHarness::new(ctx, Some(predefined_account_config)).await;

    let accounts_index_response = harness.view_accounts_index().await;
    assert_eq!(accounts_index_response.total_hits(), 2);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        vec![
            AccountBaseUseCaseHarness::account_id_from_name("jane-smith").to_string(),
            AccountBaseUseCaseHarness::account_id_from_name("john-doe").to_string(),
        ]
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "jane-smith",
                account_search_schema::fields::CREATED_AT: registered_at.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "jane-smith",
                account_search_schema::fields::UPDATED_AT: registered_at.to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "john-doe",
                account_search_schema::fields::CREATED_AT: registered_at.to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "John Doe",
                account_search_schema::fields::UPDATED_AT: registered_at.to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_accounts_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = AccountIndexingHarness::new(ctx, None).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(&harness.catalog, account_name).await;
    }

    let accounts_index_response = harness.view_accounts_index().await;

    assert_eq!(accounts_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        account_names
            .iter()
            .map(|name| AccountBaseUseCaseHarness::account_id_from_name(name).to_string())
            .collect::<Vec<_>>()
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alice",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alice",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "bob",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "bob",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_updating_account_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = AccountIndexingHarness::new(ctx, None).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(&harness.catalog, account_name).await;
    }

    harness
        .rename_account(&harness.catalog, "bob", "robert")
        .await;
    harness
        .rename_account(&harness.catalog, "alice", "alicia")
        .await;

    let accounts_index_response = harness.view_accounts_index().await;

    assert_eq!(accounts_index_response.total_hits(), 3);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        vec![
            // Althouhgh renamed, IDs remain the same.
            // However, the order changes, as we sort by account_name
            AccountBaseUseCaseHarness::account_id_from_name("alice").to_string(),
            AccountBaseUseCaseHarness::account_id_from_name("charlie").to_string(),
            AccountBaseUseCaseHarness::account_id_from_name("bob").to_string(),
        ]
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alicia",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alicia",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "robert",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "robert",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_deleting_account_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = AccountIndexingHarness::new(ctx, None).await;

    let account_names = vec!["alice", "bob", "charlie"];
    for account_name in &account_names {
        harness.create_account(&harness.catalog, account_name).await;
    }

    harness.delete_account(&harness.catalog, "bob").await;

    let accounts_index_response = harness.view_accounts_index().await;
    assert_eq!(accounts_index_response.total_hits(), 2);

    pretty_assertions::assert_eq!(
        accounts_index_response.ids(),
        vec![
            AccountBaseUseCaseHarness::account_id_from_name("alice").to_string(),
            AccountBaseUseCaseHarness::account_id_from_name("charlie").to_string(),
        ]
    );

    pretty_assertions::assert_eq!(
        accounts_index_response.entities(),
        [
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "alice",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "alice",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                account_search_schema::fields::ACCOUNT_NAME: "charlie",
                account_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                account_search_schema::fields::DISPLAY_NAME: "charlie",
                account_search_schema::fields::UPDATED_AT: harness.fixed_time().to_rfc3339(),
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(AccountBaseUseCaseHarness, account_base_harness)]
struct AccountIndexingHarness {
    es_base_harness: ElasticsearchBaseHarness,
    account_base_harness: AccountBaseUseCaseHarness,
    catalog: dill::Catalog,
}

impl AccountIndexingHarness {
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
    ) -> Self {
        let es_base_harness = ElasticsearchBaseHarness::new(ctx);

        let account_base_harness = AccountBaseUseCaseHarness::new(AccountBaseUseCaseHarnessOpts {
            maybe_base_catalog: Some(es_base_harness.catalog()),
            system_time_source_provider: time_source::SystemTimeSourceProvider::Inherited,
            maybe_predefined_accounts_config,
            ..Default::default()
        });

        let mut b = dill::CatalogBuilder::new_chained(account_base_harness.intermediate_catalog());
        b.add_builder(
            messaging_outbox::OutboxImmediateImpl::builder()
                .with_consumer_filter(messaging_outbox::ConsumerFilter::AllConsumers),
        )
        .bind::<dyn Outbox, OutboxImmediateImpl>()
        .add::<AccountSearchSchemaProvider>()
        .add::<AccountSearchUpdater>();

        database_common::NoOpDatabasePlugin::init_database_components(&mut b);

        register_message_dispatcher::<AccountLifecycleMessage>(
            &mut b,
            MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
        );

        let catalog = b.build();

        init_on_startup::run_startup_jobs(&catalog).await.unwrap();

        ElasticsearchBaseHarness::run_initial_indexing(&catalog).await;

        Self {
            es_base_harness,
            account_base_harness,
            catalog,
        }
    }

    #[inline]
    pub fn fixed_time(&self) -> DateTime<Utc> {
        self.es_base_harness.fixed_time()
    }

    pub async fn view_accounts_index(&self) -> SearchTestResponse {
        self.es_base_harness.es_ctx().refresh_indices().await;

        let search_repo = self.es_base_harness.es_ctx().search_repo();

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
