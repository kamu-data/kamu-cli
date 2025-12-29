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
use chrono::{DateTime, Utc};
use database_common::NoOpDatabasePlugin;
use init_on_startup::{InitOnStartup, InitOnStartupMeta};
use internal_error::InternalError;
use kamu_accounts::*;
use kamu_accounts_services::utils::AccountAuthorizationHelperImpl;
use kamu_accounts_services::*;
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::*;
use kamu_core::TenancyConfig;
use kamu_datasets::*;
use kamu_datasets_services::utils::CreateDatasetUseCaseHelper;
use kamu_datasets_services::*;
use kamu_messaging_outbox_inmem::{
    InMemoryOutboxMessageConsumptionRepository,
    InMemoryOutboxMessageRepository,
};
use kamu_search::*;
use kamu_search_elasticsearch::testing::{
    ElasticsearchBaseHarness,
    ElasticsearchTestContext,
    SearchTestResponse,
};
use kamu_search_services::SearchIndexer;
use messaging_outbox::*;
use odf::metadata::testing::MetadataFactory;
use time_source::{SystemTimeSource, SystemTimeSourceProvider};

use crate::tests::use_cases::dataset_base_use_case_harness::{
    DatasetBaseUseCaseHarness,
    DatasetBaseUseCaseHarnessOpts,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(elasticsearch)]
#[test_log::test(kamu_search_elasticsearch::test)]
async fn test_dataset_index_initially_empty(ctx: Arc<ElasticsearchTestContext>) {
    let harness = DatasetIndexingHarness::builder()
        .ctx(ctx)
        .tenancy_config(TenancyConfig::SingleTenant)
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 0);
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
        .maybe_predefined_datasets_config(predefined_datasets_config)
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 2);

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
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "foo",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "foo",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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
        .maybe_predefined_accounts_config(PredefinedAccountsConfig {
            predefined: account_names
                .into_iter()
                .map(AccountConfig::test_config_from_name)
                .collect(),
        })
        .maybe_predefined_datasets_config(PredefinedDatasetsConfig {
            aliases: aliases.clone(),
        })
        .build()
        .await;

    let dataset_index_response = harness.view_datasets_index().await;
    assert_eq!(dataset_index_response.total_hits(), 2);

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
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("alice".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/foo",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "foo",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("bob".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 3);

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
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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
        .maybe_predefined_accounts_config(PredefinedAccountsConfig {
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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 3);

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
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("alice".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("bob".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alice/gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("alice".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alice",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 3);

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
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "test-alpha",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "test-alpha",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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

    let datasets_index_response = harness.view_datasets_index().await;

    assert_eq!(datasets_index_response.total_hits(), 2);

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
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: DEFAULT_ACCOUNT_ID.to_string(),
                dataset_search_schema::fields::OWNER_NAME: DEFAULT_ACCOUNT_NAME_STR,
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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
        .maybe_predefined_accounts_config(PredefinedAccountsConfig {
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

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    harness.rename_account("alice", "alicia").await;

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 3);

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
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("alice".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alicia",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "bob/beta",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "beta",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("bob".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "bob",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
            }),
            serde_json::json!({
                dataset_search_schema::fields::ALIAS: "alicia/gamma",
                dataset_search_schema::fields::CREATED_AT: harness.fixed_time().to_rfc3339(),
                dataset_search_schema::fields::DATASET_NAME: "gamma",
                dataset_search_schema::fields::KIND: dataset_search_schema::fields::values::KIND_ROOT,
                // ID remains the same
                dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("alice".as_bytes()).to_string(),
                dataset_search_schema::fields::OWNER_NAME: "alicia",
                dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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
        .maybe_predefined_accounts_config(PredefinedAccountsConfig {
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

    // Force outbox processing to ensure search index is up to date
    harness.process_outbox_messages().await;

    harness.delete_account("alice").await;

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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
            dataset_search_schema::fields::OWNER_ID: odf::AccountID::new_seeded_ed25519("bob".as_bytes()).to_string(),
            dataset_search_schema::fields::OWNER_NAME: "bob",
            dataset_search_schema::fields::REF_CHANGED_AT: harness.fixed_time().to_rfc3339(),
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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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

    let datasets_index_response = harness.view_datasets_index().await;
    assert_eq!(datasets_index_response.total_hits(), 1);

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
        }),]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(DatasetBaseUseCaseHarness, dataset_base_use_case_harness)]
struct DatasetIndexingHarness {
    es_base_harness: ElasticsearchBaseHarness,
    dataset_base_use_case_harness: DatasetBaseUseCaseHarness,
    outbox_agent: Arc<dyn OutboxAgent>,
}

#[bon]
impl DatasetIndexingHarness {
    #[builder]
    async fn new(
        ctx: Arc<ElasticsearchTestContext>,
        tenancy_config: TenancyConfig,
        maybe_predefined_accounts_config: Option<PredefinedAccountsConfig>,
        maybe_predefined_datasets_config: Option<PredefinedDatasetsConfig>,
    ) -> Self {
        let es_base_harness = ElasticsearchBaseHarness::new(ctx);

        let indexing_catalog = {
            let mut b = dill::CatalogBuilder::new_chained(es_base_harness.catalog());
            // Outbox repositories
            b.add::<InMemoryOutboxMessageRepository>();
            b.add::<InMemoryOutboxMessageConsumptionRepository>();

            // Search
            b.add::<DatasetSearchSchemaProvider>();
            b.add::<DatasetSearchUpdater>();

            // Supplementary use cases
            b.add::<DatasetAccountLifecycleHandler>();
            b.add::<DeleteDatasetUseCaseImpl>();

            register_message_dispatcher::<AccountLifecycleMessage>(
                &mut b,
                MESSAGE_PRODUCER_KAMU_ACCOUNTS_SERVICE,
            );

            NoOpDatabasePlugin::init_database_components(&mut b);

            b.build()
        };

        let dataset_base_use_case_harness =
            DatasetBaseUseCaseHarness::new(DatasetBaseUseCaseHarnessOpts {
                maybe_base_catalog: Some(&indexing_catalog),
                tenancy_config,
                outbox_provider: OutboxProvider::Dispatching,
                system_time_source_provider: SystemTimeSourceProvider::Inherited,
                ..Default::default()
            })
            .await;

        let no_subject_catalog = dataset_base_use_case_harness.no_subject_catalog();
        let system_user_catalog = dataset_base_use_case_harness.intermediate_catalog();

        // Ensure search indexes exist: this is not a normal startup path,
        //  but tests need it for "predefined" content
        let search_indexer = system_user_catalog.get_one::<SearchIndexer>().unwrap();
        search_indexer.ensure_indexes_exist().await.unwrap();

        // Run predefined accounts registration, if specified
        if let Some(predefined_accounts_config) = maybe_predefined_accounts_config {
            let catalog = {
                let mut b = dill::CatalogBuilder::new_chained(no_subject_catalog);
                b.add_value(predefined_accounts_config);
                b.add::<PredefinedAccountsRegistrator>();
                b.add::<LoginPasswordAuthProvider>();
                b.add::<RebacServiceImpl>();
                b.add::<InMemoryRebacRepository>();
                b.add::<CreateAccountUseCaseImpl>();
                b.add::<UpdateAccountUseCaseImpl>();
                b.add_value(DefaultAccountProperties::default());
                b.add_value(DefaultDatasetProperties::default());
                b.build()
            };

            let predefined_accounts_registrator =
                catalog.get_one::<PredefinedAccountsRegistrator>().unwrap();
            predefined_accounts_registrator
                .run_initialization()
                .await
                .unwrap();
        }

        // Run predefined datasets registration, if specified
        if let Some(predefined_datasets_config) = maybe_predefined_datasets_config {
            let catalog = {
                let mut b = dill::CatalogBuilder::new_chained(no_subject_catalog);
                b.add::<CreateDatasetUseCaseImpl>();
                b.add::<CreateDatasetUseCaseHelper>();
                b.add_value(predefined_datasets_config);
                b.add::<PredefinedDatasetsRegistrator>();
                b.build()
            };

            let predefined_datasets_registrator =
                catalog.get_one::<PredefinedDatasetsRegistrator>().unwrap();
            predefined_datasets_registrator
                .run_initialization()
                .await
                .unwrap();
        }

        // Initialize outbox agent
        let outbox_agent = system_user_catalog.get_one::<dyn OutboxAgent>().unwrap();
        outbox_agent.run_initialization().await.unwrap();

        // Ensure search indexes are up to date after predefined datasets creation
        ElasticsearchBaseHarness::run_initial_indexing(system_user_catalog).await;

        Self {
            es_base_harness,
            dataset_base_use_case_harness,
            outbox_agent,
        }
    }

    #[inline]
    fn fixed_time(&self) -> DateTime<Utc> {
        self.es_base_harness.fixed_time()
    }

    #[inline]
    fn system_user_catalog(&self) -> &dill::Catalog {
        self.dataset_base_use_case_harness.intermediate_catalog()
    }

    #[inline]
    fn no_subject_catalog(&self) -> &dill::Catalog {
        self.dataset_base_use_case_harness.no_subject_catalog()
    }

    async fn create_mt_datasets(
        &self,
        account_dataset_names: &[(&str, &str)],
    ) -> Vec<odf::DatasetID> {
        let mut dataset_ids = Vec::with_capacity(account_dataset_names.len());
        for (account_name, dataset_name) in account_dataset_names {
            let account_name = odf::AccountName::new_unchecked(account_name);

            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(self.no_subject_catalog());
                b.add_value(CurrentAccountSubject::new_test_with(&account_name));
                b.build()
            };

            let alias = odf::DatasetAlias::new(
                Some(account_name.clone()),
                odf::DatasetName::new_unchecked(dataset_name),
            );
            dataset_ids.push(
                self.create_root_dataset(&user_specific_catalog, &alias)
                    .await
                    .dataset_handle
                    .id,
            );
        }

        dataset_ids
    }

    async fn rename_account(&self, old_name: &str, new_name: &str) {
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

                b.add::<RebacServiceImpl>();
                b.add::<InMemoryRebacRepository>();
                b.add_value(DefaultAccountProperties::default());
                b.add_value(DefaultDatasetProperties::default());

                b.build()
            };

            let update_account_uc = user_specific_catalog
                .get_one::<dyn UpdateAccountUseCase>()
                .unwrap();
            update_account_uc.execute(&updated_account).await.unwrap();
        };
    }

    async fn delete_account(&self, account_name_str: &str) {
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

                b.add::<RebacServiceImpl>();
                b.add::<InMemoryRebacRepository>();
                b.add_value(DefaultAccountProperties::default());
                b.add_value(DefaultDatasetProperties::default());

                b.build()
            };

            let delete_account_uc = user_specific_catalog
                .get_one::<dyn DeleteAccountUseCase>()
                .unwrap();
            delete_account_uc.execute(&account).await.unwrap();
        };
    }

    async fn process_outbox_messages(&self) {
        self.outbox_agent.run_while_has_tasks().await.unwrap();
    }

    async fn view_datasets_index(&self) -> SearchTestResponse {
        self.process_outbox_messages().await;
        self.es_base_harness.es_ctx().refresh_indices().await;

        let search_repo = self.es_base_harness.es_ctx().search_repo();

        let seach_response = search_repo
            .search(SearchRequest {
                query: None,
                entity_schemas: vec![dataset_search_schema::SCHEMA_NAME],
                source: SearchRequestSourceSpec::All,
                filter: None,
                sort: sort!(dataset_search_schema::fields::DATASET_NAME),
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

#[derive(Clone)]
struct PredefinedDatasetsConfig {
    aliases: Vec<odf::DatasetAlias>,
}

#[dill::component(pub)]
#[dill::interface(dyn InitOnStartup)]
#[dill::meta(InitOnStartupMeta {
    job_name: "dev.kamu.datasets.test.PredefinedDatasetsRegistrator",
    depends_on: &[],
    requires_transaction: true,
})]
struct PredefinedDatasetsRegistrator {
    time_source: Arc<dyn SystemTimeSource>,
    config: Arc<PredefinedDatasetsConfig>,
    catalog: dill::Catalog,
}

#[async_trait::async_trait]
impl InitOnStartup for PredefinedDatasetsRegistrator {
    async fn run_initialization(&self) -> Result<(), InternalError> {
        // Create predefined datasets in silense
        for alias in &self.config.aliases {
            let user_specific_catalog = {
                let mut b = dill::CatalogBuilder::new_chained(&self.catalog);
                if let Some(account_name) = &alias.account_name {
                    b.add_value(CurrentAccountSubject::new_test_with(&account_name));
                } else {
                    b.add_value(CurrentAccountSubject::new_test());
                }
                b.build()
            };

            let create_dataset_use_case = user_specific_catalog
                .get_one::<dyn CreateDatasetUseCase>()
                .unwrap();

            create_dataset_use_case
                .execute(
                    alias,
                    MetadataFactory::metadata_block(
                        MetadataFactory::seed(odf::DatasetKind::Root)
                            .id(odf::DatasetID::new_seeded_ed25519(
                                alias.dataset_name.as_str().as_bytes(),
                            ))
                            .build(),
                    )
                    .system_time(self.time_source.now())
                    .build_typed(),
                    CreateDatasetUseCaseOptions {
                        dataset_visibility: odf::DatasetVisibility::Public,
                    },
                )
                .await
                .unwrap();
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
