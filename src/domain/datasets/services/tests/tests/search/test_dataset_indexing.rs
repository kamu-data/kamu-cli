// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use bon::bon;
use kamu_accounts::*;
use kamu_accounts_services::utils::AccountAuthorizationHelperImpl;
use kamu_accounts_services::{DeleteAccountUseCaseImpl, UpdateAccountUseCaseImpl};
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_search_elasticsearch::testing::ElasticsearchTestContext;
use odf::metadata::testing::MetadataFactory;

use super::es_dataset_base_harness::{ElasticsearchDatasetBaseHarness, PredefinedDatasetsConfig};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_dataset_index_initially_empty(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(dataset_index_response.total_hits(), Some(0));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_predefined_st_datasets_indexed_properly(ctx: Arc<ElasticsearchTestContext>) {
    let aliases = vec![
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar")),
        odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo")),
    ];

    let predefined_datasets_config = PredefinedDatasetsConfig {
        aliases: aliases.clone(),
    };

    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .predefined_datasets_config(predefined_datasets_config)
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(dataset_index_response.total_hits(), Some(2));

    pretty_assertions::assert_eq!(
        dataset_index_response.ids(),
        aliases
            .iter()
            .map(|alias| {
                odf::DatasetID::new_seeded_ed25519(alias.dataset_name.as_str().as_bytes())
                    .to_string()
            })
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        dataset_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bar",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "bar",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "foo",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "foo",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            })
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_predefined_mt_datasets_indexed_properly(ctx: Arc<ElasticsearchTestContext>) {
    let account_names = vec![
        odf::AccountName::new_unchecked("alice"),
        odf::AccountName::new_unchecked("bob"),
    ];

    let alice_id = odf::AccountID::new_seeded_ed25519("alice".as_bytes());
    let bob_id = odf::AccountID::new_seeded_ed25519("bob".as_bytes());

    let aliases = vec![
        odf::DatasetAlias::new(
            Some(account_names[0].clone()),
            odf::DatasetName::new_unchecked("bar"),
        ),
        odf::DatasetAlias::new(
            Some(account_names[1].clone()),
            odf::DatasetName::new_unchecked("foo"),
        ),
    ];

    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_accounts_config(PredefinedAccountsConfig {
            predefined: account_names
                .into_iter()
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .predefined_datasets_config(PredefinedDatasetsConfig {
            aliases: aliases.clone(),
        })
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(dataset_index_response.total_hits(), Some(2));

    pretty_assertions::assert_eq!(
        dataset_index_response.ids(),
        aliases
            .iter()
            .map(|alias| {
                odf::DatasetID::new_seeded_ed25519(alias.dataset_name.as_str().as_bytes())
                    .to_string()
            })
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        dataset_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alice/bar",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "bar",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/foo",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "foo",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![bob_id.to_string()],
            })
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_st_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(harness.system_user_catalog(), &alias)
                .await
                .dataset_handle
                .id,
        );
    }

    let datasets_index_response = harness.view_datasets_index_as_admin().await;

    assert_eq!(datasets_index_response.total_hits(), Some(3));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        dataset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_creating_mt_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_accounts_config(PredefinedAccountsConfig {
            predefined: ["alice", "bob"]
                .into_iter()
                .map(odf::AccountName::new_unchecked)
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .build()
        .await;

    let dataset_ids = harness
        .create_mt_datasets(&[("alice", "alpha"), ("bob", "beta"), ("alice", "gamma")])
        .await;

    let alice_id = odf::AccountID::new_seeded_ed25519("alice".as_bytes());
    let bob_id = odf::AccountID::new_seeded_ed25519("bob".as_bytes());

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(3));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        dataset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alice/alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![bob_id.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alice/gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string()],
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_renaming_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(harness.system_user_catalog(), &alias)
                .await
                .dataset_handle
                .id,
        );
    }

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    harness
        .rename_dataset(
            harness.system_user_catalog(),
            &dataset_ids[1],
            "beta-renamed",
        )
        .await;
    harness
        .rename_dataset(harness.system_user_catalog(), &dataset_ids[0], "test-alpha")
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;

    assert_eq!(datasets_index_response.total_hits(), Some(3));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![
            // Order changed due to renaming of "alpha" to "test-alpha"
            dataset_ids[1].to_string(),
            dataset_ids[2].to_string(),
            dataset_ids[0].to_string()
        ]
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta-renamed",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta-renamed",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "test-alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "test-alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_deleting_datasets_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let dataset_names = vec!["alpha", "beta", "gamma"];
    let mut dataset_ids = Vec::with_capacity(dataset_names.len());
    for dataset_name in &dataset_names {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(dataset_name));
        dataset_ids.push(
            harness
                .create_root_dataset(harness.system_user_catalog(), &alias)
                .await
                .dataset_handle
                .id,
        );
    }

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    // Delete "beta" dataset
    harness
        .delete_dataset(harness.system_user_catalog(), &dataset_ids[1])
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;

    assert_eq!(datasets_index_response.total_hits(), Some(2));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![dataset_ids[0].to_string(), dataset_ids[2].to_string(),]
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_account_rename_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_accounts_config(PredefinedAccountsConfig {
            predefined: ["alice", "bob"]
                .into_iter()
                .map(odf::AccountName::new_unchecked)
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .build()
        .await;

    let dataset_ids = harness
        .create_mt_datasets(&[("alice", "alpha"), ("bob", "beta"), ("alice", "gamma")])
        .await;

    let alice_id = odf::AccountID::new_seeded_ed25519("alice".as_bytes());
    let bob_id = odf::AccountID::new_seeded_ed25519("bob".as_bytes());

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    harness.rename_account("alice", "alicia").await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(3));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        dataset_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alicia/alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                // ID remains the same
                dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alicia",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![bob_id.to_string()],
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alicia/gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                // ID remains the same
                dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alicia",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
                kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
                kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string()],
            }),
        ]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_account_delete_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_accounts_config(PredefinedAccountsConfig {
            predefined: ["alice", "bob"]
                .into_iter()
                .map(odf::AccountName::new_unchecked)
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .build()
        .await;

    let dataset_ids = harness
        .create_mt_datasets(&[("alice", "alpha"), ("bob", "beta"), ("alice", "gamma")])
        .await;

    let bob_id = odf::AccountID::new_seeded_ed25519("bob".as_bytes());

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    harness.delete_account("alice").await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![dataset_ids[1].to_string()],
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "bob/beta",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "beta",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
            dataset_search_schema::fields::OWNER_NAME: "bob",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![bob_id.to_string()],
        }),]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_index_dataset_with_all_kinds_of_metadata(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let foo_created = harness
        .create_root_dataset(harness.system_user_catalog(), &foo_alias)
        .await;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![
                odf::MetadataEvent::SetDataSchema(
                    MetadataFactory::set_data_schema()
                        .schema_from_arrow(&Schema::new(vec![
                            Field::new("offset", DataType::Int64, false),
                            Field::new("op", DataType::Int32, false),
                            Field::new(
                                "system_time",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                false,
                            ),
                            Field::new(
                                "event_time",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                true,
                            ),
                            Field::new("city", DataType::Utf8, true),
                            Field::new("population", DataType::Int64, true),
                        ]))
                        .build(),
                ),
                odf::MetadataEvent::SetAttachments(odf::metadata::SetAttachments {
                    attachments: odf::metadata::Attachments::Embedded(
                        odf::metadata::AttachmentsEmbedded {
                            items: vec![odf::metadata::AttachmentEmbedded {
                                path: "README.md".to_string(),
                                content: "foo dataset readme".to_string(),
                            }],
                        },
                    ),
                }),
                odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .description("Nice root dataset")
                        .keyword("test")
                        .keyword("nice")
                        .build(),
                ),
            ],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![foo_created.dataset_handle.id.to_string()],
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: "Nice root dataset",
            dataset_search_schema::fields::KEYWORDS: vec!["test", "nice"],
            dataset_search_schema::fields::ATTACHMENTS: vec!["foo dataset readme"],
            dataset_search_schema::fields::SCHEMA_FIELDS: vec![
                "city",
                "population",
            ],
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_partial_updates_all_kinds_of_metadata(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let foo_created = harness
        .create_root_dataset(harness.system_user_catalog(), &foo_alias)
        .await;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    ////////////////////////////////////////////////////////////////
    // 1. Append SetInfo: description + keywords
    ////////////////////////////////////////////////////////////////

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![odf::MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .description("Nice root dataset")
                    .keyword("test")
                    .keyword("nice")
                    .build(),
            )],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.ids(),
        vec![foo_created.dataset_handle.id.to_string()],
    );

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: "Nice root dataset",
            dataset_search_schema::fields::KEYWORDS: vec!["test", "nice"],
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );

    ////////////////////////////////////////////////////////////////
    // 2. Append SetDataSchema to existing fields
    ////////////////////////////////////////////////////////////////

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![odf::MetadataEvent::SetDataSchema(
                MetadataFactory::set_data_schema()
                    .schema_from_arrow(&Schema::new(vec![
                        Field::new("offset", DataType::Int64, false),
                        Field::new("op", DataType::Int32, false),
                        Field::new(
                            "system_time",
                            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                            false,
                        ),
                        Field::new(
                            "event_time",
                            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                            true,
                        ),
                        Field::new("city", DataType::Utf8, true),
                        Field::new("population", DataType::Int64, true),
                    ]))
                    .build(),
            )],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: "Nice root dataset",
            dataset_search_schema::fields::KEYWORDS: vec!["test", "nice"],
            dataset_search_schema::fields::SCHEMA_FIELDS: vec![
                // Note: no default fields in the index intentionally
                "city",
                "population",
            ],
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );

    ////////////////////////////////////////////////////////////////
    // 3. Append SetAttachments + clear keywords
    ////////////////////////////////////////////////////////////////

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![
                odf::MetadataEvent::SetAttachments(odf::metadata::SetAttachments {
                    attachments: odf::metadata::Attachments::Embedded(
                        odf::metadata::AttachmentsEmbedded {
                            items: vec![odf::metadata::AttachmentEmbedded {
                                path: "README.md".to_string(),
                                content: "foo dataset readme".to_string(),
                            }],
                        },
                    ),
                }),
                odf::MetadataEvent::SetInfo(
                    MetadataFactory::set_info()
                        .description("Nice root dataset")
                        .build(),
                ),
            ],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: "Nice root dataset",
            dataset_search_schema::fields::KEYWORDS: serde_json::Value::Null,
            dataset_search_schema::fields::SCHEMA_FIELDS: vec![
                // Note: no default fields in the index intentionally
                "city",
                "population",
            ],
            dataset_search_schema::fields::ATTACHMENTS: vec!["foo dataset readme"],
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );

    ////////////////////////////////////////////////////////////////
    // 4. Clear description, but add a keyword
    ////////////////////////////////////////////////////////////////

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![odf::MetadataEvent::SetInfo(
                MetadataFactory::set_info()
                    .keyword("updated-keyword")
                    .build(),
            )],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: serde_json::Value::Null,
            dataset_search_schema::fields::KEYWORDS: serde_json::json!(["updated-keyword"]),
            dataset_search_schema::fields::SCHEMA_FIELDS: vec![
                // Note: no default fields in the index intentionally
                "city",
                "population",
            ],
            dataset_search_schema::fields::ATTACHMENTS: vec!["foo dataset readme"],
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );

    ////////////////////////////////////////////////////////////////
    // 5. Clear attachment, update schema
    ////////////////////////////////////////////////////////////////

    harness
        .append_dataset_metadata(
            harness.system_user_catalog(),
            ResolvedDataset::from_created(&foo_created),
            vec![
                odf::MetadataEvent::SetAttachments(odf::metadata::SetAttachments {
                    attachments: odf::metadata::Attachments::Embedded(
                        odf::metadata::AttachmentsEmbedded { items: vec![] },
                    ),
                }),
                odf::MetadataEvent::SetDataSchema(
                    MetadataFactory::set_data_schema()
                        .schema_from_arrow(&Schema::new(vec![
                            Field::new("offset", DataType::Int64, false),
                            Field::new("op", DataType::Int32, false),
                            Field::new(
                                "system_time",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                false,
                            ),
                            Field::new(
                                "event_time",
                                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                                true,
                            ),
                            Field::new("city", DataType::Utf8, true),
                            Field::new("population", DataType::Int64, true),
                            Field::new("country", DataType::Utf8, true),
                        ]))
                        .build(),
                ),
            ],
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DESCRIPTION: serde_json::Value::Null,
            dataset_search_schema::fields::KEYWORDS: serde_json::json!(["updated-keyword"]),
            dataset_search_schema::fields::SCHEMA_FIELDS: vec![
                // Note: no default fields in the index intentionally
                "city",
                "population",
                "country",
            ],
            dataset_search_schema::fields::ATTACHMENTS: serde_json::Value::Null,
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        }),]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_visibility_updates_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let foo_created = harness
        .create_root_dataset(harness.system_user_catalog(), &foo_alias)
        .await;

    // Ensure initial indexing is done
    harness.synchronize().await;

    // Switch to public visibility
    harness
        .set_dataset_visibility(
            &foo_created.dataset_handle.id,
            odf::dataset::DatasetVisibility::Public,
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PUBLIC_GUEST,
            kamu_search::fields::PRINCIPAL_IDS: serde_json::Value::Null,
        })]
    );

    // Back to private visibility

    harness
        .set_dataset_visibility(
            &foo_created.dataset_handle.id,
            odf::dataset::DatasetVisibility::Private,
        )
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(1));

    pretty_assertions::assert_eq!(
        datasets_index_response.entities(),
        [serde_json::json!({
            dataset_search_schema::fields::ALIAS: "foo",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "foo",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
            dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![DEFAULT_ACCOUNT_ID.to_string()],
        })]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_collaboration_updates_reflected_in_index(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::MultiTenant)
        .predefined_accounts_config(PredefinedAccountsConfig {
            predefined: ["alice", "bob", "charlie"]
                .into_iter()
                .map(odf::AccountName::new_unchecked)
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .build()
        .await;

    let dataset_ids = harness
        .create_mt_datasets(&[("alice", "alpha"), ("bob", "beta")])
        .await;

    let alpha_id = dataset_ids.first().unwrap();
    let beta_id = dataset_ids.get(1).unwrap();

    let alice_id = odf::AccountID::new_seeded_ed25519("alice".as_bytes());
    let bob_id = odf::AccountID::new_seeded_ed25519("bob".as_bytes());
    let charlie_id = odf::AccountID::new_seeded_ed25519("charlie".as_bytes());

    // Ensure initial indexing is done
    harness.synchronize().await;

    // Add collaborators

    harness
        .add_dataset_collaborators(alpha_id, &[&bob_id, &charlie_id])
        .await;

    harness
        .add_dataset_collaborators(beta_id, &[&alice_id])
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(2));

    let mut actual_entities = datasets_index_response.entities();
    DatasetIndexingHarness::sort_principal_ids_in_entities(&mut actual_entities);

    let mut expected_entities = vec![
        serde_json::json!({
            dataset_search_schema::fields::ALIAS: "alice/alpha",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "alpha",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
            dataset_search_schema::fields::OWNER_NAME: "alice",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string(), bob_id.to_string(), charlie_id.to_string()],
        }),
        serde_json::json!({
            dataset_search_schema::fields::ALIAS: "bob/beta",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "beta",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
            dataset_search_schema::fields::OWNER_NAME: "bob",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string(), bob_id.to_string()],
        }),
    ];
    DatasetIndexingHarness::sort_principal_ids_in_entities(&mut expected_entities);
    pretty_assertions::assert_eq!(actual_entities, expected_entities);

    // Let's remove Bob from Alice's dataset collaborators
    harness
        .remove_dataset_collaborators(alpha_id, &[&bob_id])
        .await;

    // Let's remove Alice from Bob's dataset collaborators
    harness
        .remove_dataset_collaborators(beta_id, &[&alice_id])
        .await;

    let datasets_index_response = harness.view_datasets_index_as_admin().await;
    assert_eq!(datasets_index_response.total_hits(), Some(2));

    let mut actual_entities = datasets_index_response.entities();
    DatasetIndexingHarness::sort_principal_ids_in_entities(&mut actual_entities);

    let mut expected_entities = vec![
        serde_json::json!({
            dataset_search_schema::fields::ALIAS: "alice/alpha",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "alpha",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: alice_id.to_string(),
            dataset_search_schema::fields::OWNER_NAME: "alice",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![alice_id.to_string(), charlie_id.to_string()],
        }),
        serde_json::json!({
            dataset_search_schema::fields::ALIAS: "bob/beta",
            dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
            dataset_search_schema::fields::DATASET_NAME: "beta",
            dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
            dataset_search_schema::fields::OWNER_ID: bob_id.to_string(),
            dataset_search_schema::fields::OWNER_NAME: "bob",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            kamu_search::fields::VISIBILITY: kamu_search::fields::values::VISIBILITY_PRIVATE,
            kamu_search::fields::PRINCIPAL_IDS: vec![bob_id.to_string()],
        }),
    ];
    DatasetIndexingHarness::sort_principal_ids_in_entities(&mut expected_entities);
    pretty_assertions::assert_eq!(actual_entities, expected_entities);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(ElasticsearchDatasetBaseHarness, es_dataset_base_use_case_harness)]
struct DatasetIndexingHarness {
    es_dataset_base_use_case_harness: ElasticsearchDatasetBaseHarness,
}

#[bon]
impl DatasetIndexingHarness {
    #[builder]
    pub async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        tenancy_config: TenancyConfig,
        predefined_accounts_config: Option<PredefinedAccountsConfig>,
        predefined_datasets_config: Option<PredefinedDatasetsConfig>,
    ) -> Self {
        let es_dataset_base_use_case_harness = ElasticsearchDatasetBaseHarness::builder()
            .ctx(ctx)
            .tenancy_config(tenancy_config)
            .maybe_predefined_accounts_config(predefined_accounts_config)
            .maybe_predefined_datasets_config(predefined_datasets_config)
            .build()
            .await;

        Self {
            es_dataset_base_use_case_harness,
        }
    }

    pub async fn rename_account(&self, old_name: &str, new_name: &str) {
        let old_name = odf::AccountName::new_unchecked(old_name);
        let new_name = odf::AccountName::new_unchecked(new_name);

        // Locate account
        let account_svc = self
            .no_subject_catalog()
            .get_one::<dyn AccountService>()
            .unwrap();
        let account = account_svc
            .account_by_name(&old_name)
            .await
            .unwrap()
            .unwrap();

        // Prepare updated account
        let mut updated_account = account.clone();
        updated_account.display_name = new_name.to_string();
        updated_account.account_name = new_name;

        // Execute update on user's behalf in authenticated context
        {
            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(self.no_subject_catalog());
                b.add_value(CurrentAccountSubject::new_test_with(&old_name));
                b.add::<AccountAuthorizationHelperImpl>();
                b.add::<UpdateAccountUseCaseImpl>();
                b.build()
            };

            let update_account_uc = user_specific_catalog
                .get_one::<dyn UpdateAccountUseCase>()
                .unwrap();
            update_account_uc.execute(&updated_account).await.unwrap();
        };
    }

    pub async fn delete_account(&self, account_name_str: &str) {
        let account_name = odf::AccountName::new_unchecked(account_name_str);

        // Locate account
        let account_svc = self
            .no_subject_catalog()
            .get_one::<dyn AccountService>()
            .unwrap();
        let account = account_svc
            .account_by_name(&account_name)
            .await
            .unwrap()
            .unwrap();

        // Execute deletion on user's behalf in authenticated context
        {
            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(self.no_subject_catalog());
                b.add_value(CurrentAccountSubject::new_test_with(&account_name));
                b.add::<AccountAuthorizationHelperImpl>();
                b.add::<DeleteAccountUseCaseImpl>();
                b.build()
            };

            let delete_account_uc = user_specific_catalog
                .get_one::<dyn DeleteAccountUseCase>()
                .unwrap();
            delete_account_uc.execute(&account).await.unwrap();
        };
    }

    fn sort_principal_ids_in_entities(entities: &mut [serde_json::Value]) {
        for entity in entities {
            if let Some(arr) = entity
                .get_mut(kamu_search::fields::PRINCIPAL_IDS)
                .and_then(|v| v.as_array_mut())
            {
                arr.sort_by(|a, b| a.as_str().unwrap_or("").cmp(b.as_str().unwrap_or("")));
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
