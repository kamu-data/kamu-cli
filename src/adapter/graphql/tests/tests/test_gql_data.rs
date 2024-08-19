// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;
use std::sync::Arc;

use database_common::{DatabaseTransactionRunner, NoOpDatabasePlugin};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use dill::Component;
use kamu::testing::{MetadataFactory, ParquetWriterHelper};
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_inmem::{InMemoryAccessTokenRepository, InMemoryAccountRepository};
use kamu_accounts_services::{
    AccessTokenServiceImpl,
    AuthenticationServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::RebacServiceImpl;
use kamu_core::*;
use messaging_outbox::DummyOutboxImpl;
use opendatafabric::*;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_local_workspace(
    tempdir: &Path,
    is_multitenant: bool,
) -> dill::Catalog {
    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let current_account_subject = CurrentAccountSubject::new_test();
    let mut predefined_accounts_config = PredefinedAccountsConfig::new();

    if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
        predefined_accounts_config
            .predefined
            .push(AccountConfig::from_name(
                logged_account.account_name.clone(),
            ));
    } else {
        panic!()
    }

    let catalog = {
        let mut b = dill::CatalogBuilder::new();

        b.add::<DependencyGraphServiceInMemory>()
            .add_value(current_account_subject)
            .add_value(predefined_accounts_config)
            .add_builder(
                DatasetRepositoryLocalFs::builder()
                    .with_root(datasets_dir)
                    .with_multi_tenant(is_multitenant),
            )
            .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
            .bind::<dyn DatasetRepositoryWriter, DatasetRepositoryLocalFs>()
            .add::<CreateDatasetUseCaseImpl>()
            .add::<DummyOutboxImpl>()
            .add::<SystemTimeSourceDefault>()
            .add::<QueryServiceImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<auth::AlwaysHappyDatasetActionAuthorizer>()
            .add::<AuthenticationServiceImpl>()
            .add::<AccessTokenServiceImpl>()
            .add::<InMemoryAccessTokenRepository>()
            .add::<InMemoryAccountRepository>()
            .add_value(JwtAuthenticationConfig::default())
            .add::<LoginPasswordAuthProvider>()
            .add::<PredefinedAccountsRegistrator>()
            .add::<DatabaseTransactionRunner>()
            .add::<InMemoryRebacRepository>()
            .add::<RebacServiceImpl>();

        NoOpDatabasePlugin::init_database_components(&mut b);

        b.build()
    };

    DatabaseTransactionRunner::new(catalog.clone())
        .transactional(|transactional_catalog| async move {
            let registrator = transactional_catalog
                .get_one::<PredefinedAccountsRegistrator>()
                .unwrap();

            registrator
                .ensure_predefined_accounts_are_registered()
                .await
        })
        .await
        .unwrap();

    catalog
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(
    catalog: &dill::Catalog,
    tempdir: &Path,
    account_name: Option<AccountName>,
) {
    let create_dataset = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

    let dataset = create_dataset
        .execute(
            &DatasetAlias::new(account_name, DatasetName::new_unchecked("foo")),
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap()
        .dataset;

    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));

    dataset
        .commit_event(
            MetadataFactory::set_data_schema()
                .schema(&schema)
                .build()
                .into(),
            CommitOpts::default(),
        )
        .await
        .unwrap();

    let a: Arc<dyn Array> = Arc::new(UInt64Array::from(vec![0, 1, 2, 3]));
    let b: Arc<dyn Array> = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let record_batch =
        RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)]).unwrap();

    // TODO: Use DataWriter
    let tmp_data_path = tempdir.join("data");
    ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

    dataset
        .commit_add_data(
            AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(OffsetInterval { start: 0, end: 3 }),
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(tmp_data_path)),
            None,
            CommitOpts::default(),
        )
        .await
        .unwrap();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tail
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_schema() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(limit: 1, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        schema { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data_schema = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["schema"]["content"];
    let data_schema =
        serde_json::from_str::<serde_json::Value>(data_schema.as_str().unwrap()).unwrap();
    assert_eq!(
        data_schema,
        serde_json::json!({
            "name": "arrow_schema",
            "type": "struct",
            "fields": [{
                "name": "offset",
                "repetition": "REQUIRED",
                "type": "INT64",
                "logicalType": "INTEGER(64,false)"
            }, {
                "name": "blah",
                "repetition": "REQUIRED",
                "type": "BYTE_ARRAY",
                "logicalType": "STRING"
            }]
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_some() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(skip: 1, limit: 1, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        data { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([{"blah": "c", "offset": 2}]));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    datasets {
                        byOwnerAndName(accountName: "kamu", datasetName: "foo") {
                            name
                            data {
                                tail(skip: 10, schemaFormat: PARQUET_JSON, dataFormat: JSON) {
                                    ... on DataQueryResultSuccess {
                                        data { content }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(data, serde_json::json!([]));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_some() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    data {
                        query(
                            query: "select * from \"kamu/foo\" order by offset",
                            queryDialect: SQL_DATA_FUSION,
                            schemaFormat: ARROW_JSON,
                            dataFormat: JSON,
                        ) {
                            ... on DataQueryResultSuccess {
                                data { content }
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["data"]["query"]["data"]["content"];
    let data = serde_json::from_str::<serde_json::Value>(data.as_str().unwrap()).unwrap();
    assert_eq!(
        data,
        serde_json::json!([
            {"offset": 0, "blah": "a"},
            {"offset": 1, "blah": "b"},
            {"offset": 2, "blah": "c"},
            {"offset": 3, "blah": "d"},
        ])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_error_sql_unparsable() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    data {
                        query(
                            query: "select ???",
                            queryDialect: SQL_DATA_FUSION,
                            schemaFormat: ARROW_JSON,
                            dataFormat: JSON,
                        ) {
                            ... on DataQueryResultError {
                                errorMessage
                                errorKind
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    tracing::debug!(?json, "Response data");
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["data"]["query"];
    assert_eq!(
        *data,
        serde_json::json!({
            "errorMessage": "sql parser error: Expected end of statement, found: ?",
            "errorKind": "INVALID_SQL",
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_error_sql_missing_function() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), true).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                    data {
                        query(
                            query: "select foobar(1)",
                            queryDialect: SQL_DATA_FUSION,
                            schemaFormat: ARROW_JSON,
                            dataFormat: JSON,
                        ) {
                            ... on DataQueryResultError {
                                errorMessage
                                errorKind
                            }
                        }
                    }
                }
                "#
            ))
            .data(catalog),
        )
        .await;
    assert!(res.is_ok(), "{res:?}");
    let json = serde_json::to_string(&res.data).unwrap();
    tracing::debug!(?json, "Response data");
    let json = serde_json::from_str::<serde_json::Value>(&json).unwrap();
    let data = &json["data"]["query"];
    assert_eq!(
        *data,
        serde_json::json!({
            "errorMessage": "Invalid function 'foobar'.\nDid you mean 'floor'?",
            "errorKind": "INVALID_SQL",
        })
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
