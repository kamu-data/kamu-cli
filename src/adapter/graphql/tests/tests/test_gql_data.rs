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
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use file_utils::OwnedFile;
use kamu::testing::ParquetWriterHelper;
use kamu::*;
use kamu_adapter_graphql::data_loader::{account_entity_data_loader, dataset_handle_data_loader};
use kamu_core::*;
use kamu_datasets::*;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{BaseGQLDatasetHarness, PredefinedAccountOpts, authentication_catalogs};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Tail
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_schema() {
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_test_dataset(None).await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc::indoc!(
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
        )))
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
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_test_dataset(None).await;

    let res = harness
        .execute_authorized_query(
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
            ,
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
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_test_dataset(None).await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc::indoc!(
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
        )))
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
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    harness.create_test_dataset(None).await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc::indoc!(
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
        )))
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
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc::indoc!(
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
        )))
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
    let harness = GraphQLDataHarness::builder()
        .tenancy_config(TenancyConfig::MultiTenant)
        .build()
        .await;

    let res = harness
        .execute_authorized_query(async_graphql::Request::new(indoc::indoc!(
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
        )))
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
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct GraphQLDataHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_authorized: dill::Catalog,
}

#[bon]
impl GraphQLDataHarness {
    #[builder]
    pub async fn new(tenancy_config: TenancyConfig) -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(tenancy_config)
            .build();

        let cache_dir = base_gql_harness.temp_dir().join("cache");
        std::fs::create_dir(&cache_dir).unwrap();

        let catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
                .add::<QueryServiceImpl>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>();

            b.build()
        };

        let (_catalog_anonymous, catalog_authorized) = authentication_catalogs(
            &catalog,
            PredefinedAccountOpts {
                is_admin: false,
                can_provision_accounts: false,
            },
        )
        .await;

        Self {
            base_gql_harness,
            catalog_authorized,
        }
    }

    pub async fn execute_authorized_query(
        &self,
        query: impl Into<async_graphql::Request>,
    ) -> async_graphql::Response {
        kamu_adapter_graphql::schema_quiet()
            .execute(
                query
                    .into()
                    .data(account_entity_data_loader(&self.catalog_authorized))
                    .data(dataset_handle_data_loader(&self.catalog_authorized))
                    .data(self.catalog_authorized.clone()),
            )
            .await
    }

    pub async fn create_test_dataset(&self, account_name: Option<odf::AccountName>) {
        let create_dataset = self
            .catalog_authorized
            .get_one::<dyn CreateDatasetUseCase>()
            .unwrap();

        let dataset = create_dataset
            .execute(
                &odf::DatasetAlias::new(account_name, odf::DatasetName::new_unchecked("foo")),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root).build(),
                )
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
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::clone(&a), Arc::clone(&b)])
                .unwrap();

        // TODO: Use DataWriter
        let tmp_data_path = self.base_gql_harness.temp_dir().join("data");
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
