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

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use file_utils::OwnedFile;
use kamu::testing::ParquetWriterHelper;
use kamu::*;
use kamu_accounts::*;
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_accounts_services::{
    AccountServiceImpl,
    LoginPasswordAuthProvider,
    PredefinedAccountsRegistrator,
};
use kamu_auth_rebac_inmem::InMemoryRebacRepository;
use kamu_auth_rebac_services::{
    DefaultAccountProperties,
    DefaultDatasetProperties,
    RebacServiceImpl,
};
use kamu_core::*;
use kamu_datasets::*;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::BaseGQLDatasetHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_local_workspace(
    tempdir: &Path,
    tenancy_config: TenancyConfig,
) -> dill::Catalog {
    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    let current_account_subject = CurrentAccountSubject::new_test();
    let mut predefined_accounts_config = PredefinedAccountsConfig::new();

    if let CurrentAccountSubject::Logged(logged_account) = &current_account_subject {
        predefined_accounts_config
            .predefined
            .push(AccountConfig::test_config_from_name(
                logged_account.account_name.clone(),
            ));
    } else {
        panic!()
    }

    let base_gql_harness = BaseGQLDatasetHarness::builder()
        .tenancy_config(tenancy_config)
        .build();

    let catalog = {
        let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

        b.add_value(current_account_subject)
            .add_value(predefined_accounts_config)
            .add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
            .add::<QueryServiceImpl>()
            .add::<ObjectStoreRegistryImpl>()
            .add::<ObjectStoreBuilderLocalFs>()
            .add::<LoginPasswordAuthProvider>()
            .add::<RebacServiceImpl>()
            .add::<InMemoryRebacRepository>()
            .add_value(DidSecretEncryptionConfig::sample())
            .add_value(DefaultAccountProperties::default())
            .add_value(DefaultDatasetProperties::default())
            .add::<PredefinedAccountsRegistrator>()
            .add::<AccountServiceImpl>()
            .add::<InMemoryAccountRepository>();

        b.build()
    };

    init_on_startup::run_startup_jobs(&catalog).await.unwrap();

    catalog
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(
    catalog: &dill::Catalog,
    tempdir: &Path,
    account_name: Option<odf::AccountName>,
) {
    let create_dataset = catalog.get_one::<dyn CreateDatasetUseCase>().unwrap();

    let dataset = create_dataset
        .execute(
            &odf::DatasetAlias::new(account_name, odf::DatasetName::new_unchecked("foo")),
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
            Default::default(),
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
                .schema_from_arrow(&schema)
                .build()
                .into(),
            odf::dataset::CommitOpts::default(),
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
            odf::dataset::AddDataParams {
                prev_checkpoint: None,
                prev_offset: None,
                new_offset_interval: Some(odf::metadata::OffsetInterval { start: 0, end: 3 }),
                new_linked_objects: None,
                new_watermark: None,
                new_source_state: None,
            },
            Some(OwnedFile::new(tmp_data_path)),
            None,
            odf::dataset::CommitOpts::default(),
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
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;
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

    let json = serde_json::to_value(&res.data).unwrap();
    let schema_content = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["schema"]["content"];
    let data_schema =
        serde_json::from_str::<serde_json::Value>(schema_content.as_str().unwrap()).unwrap();

    pretty_assertions::assert_eq!(
        json!({
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
        }),
        data_schema,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_some() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;
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

    let json = serde_json::to_value(&res.data).unwrap();
    let data_content = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data_content_json =
        serde_json::from_str::<serde_json::Value>(data_content.as_str().unwrap()).unwrap();

    pretty_assertions::assert_eq!(json!([{"blah": "c", "offset": 2}]), data_content_json);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;
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

    let json = serde_json::to_value(&res.data).unwrap();
    let data_content = &json["datasets"]["byOwnerAndName"]["data"]["tail"]["data"]["content"];
    let data_content_json =
        serde_json::from_str::<serde_json::Value>(data_content.as_str().unwrap()).unwrap();

    pretty_assertions::assert_eq!(json!([]), data_content_json);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Query
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_some() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;
    create_test_dataset(&catalog, tempdir.path(), None).await;

    let schema = kamu_adapter_graphql::schema_quiet();
    let res = schema
        .execute(
            async_graphql::Request::new(indoc::indoc!(
                r#"
                {
                  data {
                    query(
                      query: "select * from \"kamu/foo\" order by offset"
                      queryDialect: SQL_DATA_FUSION
                      schemaFormat: ARROW_JSON
                      dataFormat: JSON
                    ) {
                      ... on DataQueryResultSuccess {
                        data {
                          content
                        }
                        datasets {
                          id
                          alias
                          # blockHash
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

    let json = serde_json::to_value(&res.data).unwrap();
    let data_content = &json["data"]["query"]["data"]["content"];
    let data_content_json =
        serde_json::from_str::<serde_json::Value>(data_content.as_str().unwrap()).unwrap();

    pretty_assertions::assert_eq!(
        json!([
            {"offset": 0, "blah": "a"},
            {"offset": 1, "blah": "b"},
            {"offset": 2, "blah": "c"},
            {"offset": 3, "blah": "d"},
        ]),
        data_content_json
    );
    pretty_assertions::assert_eq!(
        json!([
            {
                "alias": "kamu/foo",
                "id": "did:odf:fed01df230b49615d175307d580c33d6fda61fc7b9aec91df0f5c1a5ebe3b8cbfee02"
            }
        ]),
        json["data"]["query"]["datasets"]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_error_sql_unparsable() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;

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

    let json = serde_json::to_value(&res.data).unwrap();

    pretty_assertions::assert_eq!(
        json!({
            "errorMessage": "sql parser error: Expected: end of statement, found: ? at Line: 1, Column: 9",
            "errorKind": "INVALID_SQL",
        }),
        json["data"]["query"],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_data_query_error_sql_missing_function() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), TenancyConfig::MultiTenant).await;

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

    let json = serde_json::to_value(&res.data).unwrap();

    pretty_assertions::assert_eq!(
        json!({
            "errorMessage": "Invalid function 'foobar'.\nDid you mean 'floor'?",
            "errorKind": "INVALID_SQL",
        }),
        json["data"]["query"],
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
