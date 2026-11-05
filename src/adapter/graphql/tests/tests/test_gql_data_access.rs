// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use datafusion::arrow::array::{StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use file_utils::OwnedFile;
use indoc::indoc;
use kamu::testing::ParquetWriterHelper;
use kamu::*;
use kamu_accounts::{CurrentAccountSubject, DEFAULT_ACCOUNT_NAME};
use kamu_auth_rebac::{AccountPropertyName, RebacService, boolean_property_value};
use kamu_core::*;
use kamu_datasets::*;
use messaging_outbox::OutboxProvider;
use odf::metadata::testing::MetadataFactory;
use serde_json::json;

use crate::utils::{
    Access,
    AccessMatrix,
    BaseGQLDatasetHarness,
    Caller,
    PredefinedAccountOpts,
    authentication_catalogs_ext,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Reading and writing dataset data through `data.query` and `Dataset.data`.
// The owner holds a private dataset, a public one, and a public one closed to
// anonymous readers. Each holds a single row with a marker value, so a caller
// can read a dataset exactly when the marker comes back.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const PRIVATE: &str = "private-dataset";
const PUBLIC: &str = "public-dataset";
const PUBLIC_NO_ANONYMOUS: &str = "public-no-anonymous";

const DATASET_NOT_ACCESSIBLE: &str = "Dataset not accessible";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Reads
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_select_by_alias() {
    let harness = DataAccessHarness::new().await;

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS] {
        let access = harness
            .sql_access_matrix(dataset, &format!(r#"select * from "kamu/{dataset}""#))
            .await;

        pretty_assertions::assert_eq!(
            access,
            DataAccessHarness::read_policy(
                dataset,
                &format!("table 'kamu.kamu.kamu/{dataset}' not found")
            ),
            "{dataset}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_select_by_id() {
    let harness = DataAccessHarness::new().await;

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS] {
        let dataset_id = harness.dataset_id(dataset);
        let access = harness
            .sql_access_matrix(dataset, &format!(r#"select * from "{dataset_id}""#))
            .await;

        pretty_assertions::assert_eq!(
            access,
            DataAccessHarness::read_policy(
                dataset,
                &format!("table 'kamu.kamu.{dataset_id}' not found")
            ),
            "{dataset}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_sql_to_table_hides_inaccessible_datasets() {
    let harness = DataAccessHarness::new().await;

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS, "missing-dataset"] {
        for caller in Caller::ALL {
            let hidden = dataset == "missing-dataset"
                || DataAccessHarness::read_policy(dataset, "").get(caller) != &Access::Allowed;

            pretty_assertions::assert_eq!(
                harness.to_table_reports_not_found(caller, dataset).await,
                hidden,
                "{caller:?} {dataset}"
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_show_tables() {
    let harness = DataAccessHarness::new().await;

    for caller in Caller::ALL {
        let tables = harness.listed_tables(caller).await;

        let expected = [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS]
            .into_iter()
            .filter(|dataset| {
                DataAccessHarness::read_policy(dataset, "").get(caller) == &Access::Allowed
            })
            .map(|dataset| format!("kamu/{dataset}"))
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(tables, expected, "{caller:?}");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail() {
    let harness = DataAccessHarness::new().await;

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS] {
        let access = harness.tail_access_matrix(dataset).await;

        pretty_assertions::assert_eq!(
            access,
            DataAccessHarness::read_policy(dataset, DATASET_NOT_ACCESSIBLE),
            "{dataset}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Writes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_writes_rejected_for_everyone() {
    let harness = DataAccessHarness::new().await;

    let copy_target = harness.temp_dir().join("copy.csv");

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS] {
        for (statement, expected_error) in [
            (
                format!(r#"insert into "kamu/{dataset}" select * from "kamu/{dataset}""#),
                "DML not supported: Insert Into",
            ),
            (
                format!(r#"delete from "kamu/{dataset}""#),
                "DML not supported: Delete",
            ),
            (
                format!(
                    r#"copy "kamu/{dataset}" to '{}' stored as csv"#,
                    copy_target.display()
                ),
                "DML not supported: COPY",
            ),
        ] {
            let access = harness.sql_access_matrix(dataset, &statement).await;

            pretty_assertions::assert_eq!(
                access,
                DataAccessHarness::read_policy(
                    dataset,
                    &format!("table 'kamu.kamu.kamu/{dataset}' not found")
                )
                .with_allowed_denied(expected_error),
                "{statement}"
            );
        }

        // DDL is rejected before the table is looked up
        let statement = format!(r#"drop table "kamu/{dataset}""#);
        let access = harness.sql_access_matrix(dataset, &statement).await;

        pretty_assertions::assert_eq!(
            access,
            AccessMatrix::nobody("DDL not supported: DropTable"),
            "{statement}"
        );
    }

    assert!(
        !copy_target.exists(),
        "COPY wrote {}",
        copy_target.display()
    );

    for dataset in [PRIVATE, PUBLIC, PUBLIC_NO_ANONYMOUS] {
        let access = harness
            .sql_access(
                Caller::Owner,
                dataset,
                &format!(r#"select * from "kamu/{dataset}""#),
            )
            .await;

        pretty_assertions::assert_eq!(access, Access::Allowed, "{dataset} changed");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[oop::extend(BaseGQLDatasetHarness, base_gql_harness)]
struct DataAccessHarness {
    base_gql_harness: BaseGQLDatasetHarness,
    catalog_anonymous: dill::Catalog,
    catalog_owner: dill::Catalog,
    catalog_other_account: dill::Catalog,
    catalog_admin: dill::Catalog,
    dataset_ids: Vec<(String, odf::DatasetID)>,
}

impl DataAccessHarness {
    async fn new() -> Self {
        let base_gql_harness = BaseGQLDatasetHarness::builder()
            .tenancy_config(TenancyConfig::MultiTenant)
            .outbox_provider(OutboxProvider::Immediate {
                force_immediate: true,
            })
            .build();

        let catalog = {
            let mut b = dill::CatalogBuilder::new_chained(base_gql_harness.catalog());

            b.add_value(EngineConfigDatafusionEmbeddedBatchQuery::default())
                .add::<QueryServiceImpl>()
                .add::<SessionContextBuilder>()
                .add::<ObjectStoreRegistryImpl>()
                .add::<ObjectStoreBuilderLocalFs>();

            b.build()
        };

        let auth =
            authentication_catalogs_ext(&catalog, None, PredefinedAccountOpts::default()).await;

        let subject_catalog = |subject: CurrentAccountSubject| {
            dill::CatalogBuilder::new_chained(&auth.catalog_no_subject)
                .add_value(subject)
                .build()
        };

        let admin_subject = CurrentAccountSubject::new_test_with(&"admin-account");
        auth.catalog_authorized
            .get_one::<dyn RebacService>()
            .unwrap()
            .set_account_property(
                admin_subject.account_id(),
                AccountPropertyName::IsAdmin,
                &boolean_property_value(true),
            )
            .await
            .unwrap();

        let mut harness = Self {
            base_gql_harness,
            catalog_anonymous: auth.catalog_anonymous,
            catalog_other_account: subject_catalog(CurrentAccountSubject::new_test_with(
                &"other-account",
            )),
            catalog_admin: subject_catalog(admin_subject),
            catalog_owner: auth.catalog_authorized,
            dataset_ids: Vec::new(),
        };

        for (dataset, visibility) in [
            (PRIVATE, odf::DatasetVisibility::Private),
            (PUBLIC, odf::DatasetVisibility::Public),
            (PUBLIC_NO_ANONYMOUS, odf::DatasetVisibility::Public),
        ] {
            let dataset_id = harness
                .create_dataset_with_marker(dataset, visibility)
                .await;
            harness.dataset_ids.push((dataset.to_string(), dataset_id));
        }

        harness
            .set_dataset_anonymous_read(PUBLIC_NO_ANONYMOUS, false)
            .await;

        harness
    }

    /// The dataset read policy: owner and admin read everything, other
    /// accounts read public datasets, anonymous callers read public datasets
    /// open to anonymous readers. Hidden datasets fail with `hidden_error`.
    fn read_policy(dataset: &str, hidden_error: &str) -> AccessMatrix {
        let hidden = || Access::denied(hidden_error);
        match dataset {
            PRIVATE => AccessMatrix {
                anonymous: hidden(),
                owner: Access::Allowed,
                other_account: hidden(),
                admin: Access::Allowed,
            },
            PUBLIC => AccessMatrix::everyone(),
            PUBLIC_NO_ANONYMOUS => AccessMatrix {
                anonymous: hidden(),
                owner: Access::Allowed,
                other_account: Access::Allowed,
                admin: Access::Allowed,
            },
            _ => unreachable!("unknown dataset {dataset}"),
        }
    }

    fn catalog_for(&self, caller: Caller) -> &dill::Catalog {
        match caller {
            Caller::Anonymous => &self.catalog_anonymous,
            Caller::Owner => &self.catalog_owner,
            Caller::OtherAccount => &self.catalog_other_account,
            Caller::Admin => &self.catalog_admin,
        }
    }

    fn dataset_id(&self, dataset: &str) -> &odf::DatasetID {
        &self
            .dataset_ids
            .iter()
            .find(|(name, _)| name == dataset)
            .unwrap()
            .1
    }

    fn marker(dataset: &str) -> String {
        format!("{dataset}-marker")
    }

    async fn create_dataset_with_marker(
        &self,
        dataset: &str,
        visibility: odf::DatasetVisibility,
    ) -> odf::DatasetID {
        let created = self
            .catalog_owner
            .get_one::<dyn CreateDatasetUseCase>()
            .unwrap()
            .execute(
                &odf::DatasetAlias::new(
                    Some(DEFAULT_ACCOUNT_NAME.clone()),
                    odf::DatasetName::new_unchecked(dataset),
                ),
                MetadataFactory::metadata_block(
                    MetadataFactory::seed(odf::DatasetKind::Root)
                        .id_from(dataset)
                        .build(),
                )
                .build_typed(),
                CreateDatasetUseCaseOptions {
                    dataset_visibility: visibility,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("offset", DataType::UInt64, false),
            Field::new("blah", DataType::Utf8, false),
        ]));

        created
            .dataset
            .commit_event(
                MetadataFactory::set_data_schema()
                    .schema_from_arrow(&schema)
                    .build()
                    .into(),
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();

        let record_batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![0])),
                Arc::new(StringArray::from(vec![Self::marker(dataset)])),
            ],
        )
        .unwrap();

        let data_path = self.temp_dir().join(format!("{dataset}.parquet"));
        ParquetWriterHelper::from_record_batch(&data_path, &record_batch).unwrap();

        created
            .dataset
            .commit_add_data(
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset: None,
                    new_offset_interval: Some(odf::metadata::OffsetInterval { start: 0, end: 0 }),
                    new_linked_objects: None,
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(data_path)),
                None,
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();

        created.dataset_handle.id
    }

    async fn set_dataset_anonymous_read(&self, dataset: &str, allows: bool) {
        self.set_anonymous_read(&self.catalog_owner, self.dataset_id(dataset), allows)
            .await;
    }

    /// Runs `sql` through `data.query` and reports whether the caller saw the
    /// dataset's marker row
    async fn sql_access(&self, caller: Caller, dataset: &str, sql: &str) -> Access {
        let response = self
            .execute_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($sql: String!) {
                        data {
                            query(query: $sql, queryDialect: SQL_DATA_FUSION) {
                                __typename
                                ... on DataQueryResultSuccess { data { content } }
                                ... on DataQueryResultError { errorMessage }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({ "sql": sql }))),
                self.catalog_for(caller),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let json = response.data.into_json().unwrap();
        let result = &json["data"]["query"];

        if let Some(error) = result["errorMessage"].as_str() {
            Access::denied(error)
        } else if result["data"]["content"]
            .as_str()
            .unwrap()
            .contains(&Self::marker(dataset))
        {
            Access::Allowed
        } else {
            Access::denied(&format!("no marker in {result}"))
        }
    }

    async fn sql_access_matrix(&self, dataset: &str, sql: &str) -> AccessMatrix {
        AccessMatrix::collect(async |caller| self.sql_access(caller, dataset, sql).await).await
    }

    /// Whether `to_table()` answers the caller as if the dataset did not exist
    async fn to_table_reports_not_found(&self, caller: Caller, dataset: &str) -> bool {
        let response = self
            .execute_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($sql: String!) {
                        data {
                            query(query: $sql, queryDialect: SQL_DATA_FUSION) {
                                ... on DataQueryResultError { errorMessage }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "sql": format!(r#"select * from to_table("kamu/{dataset}")"#),
                }))),
                self.catalog_for(caller),
            )
            .await;

        let json = response.data.into_json().unwrap();
        json["data"]["query"]["errorMessage"].as_str()
            == Some(format!("to_table(): dataset 'kamu/{dataset}' not found").as_str())
    }

    /// Reads `Dataset.data.tail` of the dataset looked up by owner and name
    async fn tail_access(&self, caller: Caller, dataset: &str) -> Access {
        let response = self
            .execute_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    query ($accountName: AccountName!, $datasetName: DatasetName!) {
                        datasets {
                            byOwnerAndName(accountName: $accountName, datasetName: $datasetName) {
                                data {
                                    tail(limit: 10, dataFormat: JSON) {
                                        ... on DataQueryResultSuccess { data { content } }
                                        ... on DataQueryResultError { errorMessage }
                                    }
                                }
                            }
                        }
                    }
                    "#
                ))
                .variables(async_graphql::Variables::from_json(json!({
                    "accountName": DEFAULT_ACCOUNT_NAME.as_str(),
                    "datasetName": dataset,
                }))),
                self.catalog_for(caller),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let json = response.data.into_json().unwrap();
        let dataset_json = &json["datasets"]["byOwnerAndName"];

        if dataset_json.is_null() {
            Access::denied(DATASET_NOT_ACCESSIBLE)
        } else if dataset_json.to_string().contains(&Self::marker(dataset)) {
            Access::Allowed
        } else {
            Access::denied(&format!("no marker in {dataset_json}"))
        }
    }

    async fn tail_access_matrix(&self, dataset: &str) -> AccessMatrix {
        AccessMatrix::collect(async |caller| self.tail_access(caller, dataset).await).await
    }

    /// Dataset tables listed by `show tables`, sorted
    async fn listed_tables(&self, caller: Caller) -> Vec<String> {
        let response = self
            .execute_query(
                async_graphql::Request::new(indoc!(
                    r#"
                    {
                        data {
                            query(query: "show tables", queryDialect: SQL_DATA_FUSION) {
                                ... on DataQueryResultSuccess { data { content } }
                                ... on DataQueryResultError { errorMessage }
                            }
                        }
                    }
                    "#
                )),
                self.catalog_for(caller),
            )
            .await;
        assert!(response.is_ok(), "{response:?}");

        let json = response.data.into_json().unwrap();
        let content = json["data"]["query"]["data"]["content"]
            .as_str()
            .unwrap_or_else(|| panic!("{json}"));

        let mut tables = serde_json::from_str::<Vec<serde_json::Value>>(content)
            .unwrap()
            .into_iter()
            .filter(|row| row["table_schema"] == "kamu")
            .map(|row| row["table_name"].as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        tables.sort();
        tables
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
