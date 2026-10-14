// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use datafusion::prelude::*;
use indoc::indoc;
use kamu::domain::*;
use kamu::*;
use kamu_datasets::{DatasetRegistry, MockDatasetActionAuthorizer, ResolvedDataset};
use kamu_ingest_datafusion::{DataWriterDataFusion, ReaderCsv};
use odf::dataset::testing::create_test_dataset_from_snapshot;
use odf::metadata::testing::MetadataFactory;
use odf::utils::testing::assert_data_eq;
use time_source::SystemTimeSource;

use crate::tests::queries::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// `to_table()` returns the state projected from a dataset's changelog: the latest record per
// primary key by offset, without retracted keys
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_several_changes_per_key_in_one_block() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                1,A,1000
                0,A,2000
                0,B,10
                2,B,10
                3,B,20
                2,B,20
                3,B,30
                0,C,1
                1,C,1
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 2      | 0  | A    | 2000       |
            | 7      | 3  | B    | 30         |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_across_blocks() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                0,B,2000
                0,C,3000
                "#
            ),
            schema,
        )
        .await;

    // Corrections and retractions refer to records of the previous block
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                2,A,1000
                3,A,1100
                1,B,2000
                "#
            ),
            schema,
        )
        .await;

    // A retracted key appears again, a live key is retracted
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,B,2100
                1,C,3000
                "#
            ),
            schema,
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +--------+----+------+------------+
            | offset | op | city | population |
            +--------+----+------+------------+
            | 4      | 3  | A    | 1100       |
            | 6      | 0  | B    | 2100       |
            +--------+----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_composite_primary_key() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            ToTableHarness::changelog_stream(&["city", "year"]),
            None,
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,year,population
                0,A,2020,1000
                0,A,2021,1100
                1,A,2020,1000
                0,B,2020,2000
                2,B,2020,2000
                3,B,2020,2500
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, year INT NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT offset, op, city, year, population FROM to_table(foo) ORDER BY city, year")
            .await,
        indoc!(
            r#"
            +--------+----+------+------+------------+
            | offset | op | city | year | population |
            +--------+----+------+------+------------+
            | 1      | 0  | A    | 2021 | 1100       |
            | 5      | 3  | B    | 2020 | 2500       |
            +--------+----+------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_changelog_stream_custom_vocabulary() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            ToTableHarness::changelog_stream(&["city"]),
            Some(odf::metadata::SetVocab {
                offset_column: Some("idx".to_string()),
                operation_type_column: Some("change".to_string()),
                ..Default::default()
            }),
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                change,city,population
                0,A,1000
                0,B,2000
                1,A,1000
                2,B,2000
                3,B,2500
                "#
            ),
            "change INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT idx, change, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +-----+--------+------+------------+
            | idx | change | city | population |
            +-----+--------+------+------------+
            | 4   | 3      | B    | 2500       |
            +-----+--------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_snapshot_from_push_source() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset_with_source(
            "foo",
            MetadataFactory::add_push_source()
                .merge(odf::metadata::MergeStrategySnapshot {
                    primary_key: vec!["city".to_string()],
                    compare_columns: None,
                })
                .build()
                .into(),
            None,
        )
        .await;
    let schema = "city STRING NOT NULL, population BIGINT";

    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1000
                B,2000
                C,3000
                "#
            ),
            schema,
        )
        .await;

    // `A` changes, `B` disappears, `D` appears
    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1100
                C,3000
                D,4000
                "#
            ),
            schema,
        )
        .await;

    assert_data_eq(
        harness
            .sql("SELECT op, city, population FROM to_table(foo) ORDER BY city")
            .await,
        indoc!(
            r#"
            +----+------+------------+
            | op | city | population |
            +----+------+------------+
            | 3  | A    | 1100       |
            | 0  | C    | 3000       |
            | 0  | D    | 4000       |
            +----+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_by_alias_and_id_return_same_state() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                2,A,1000
                3,A,1100
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    let expected = indoc!(
        r#"
        +--------+------+------------+
        | offset | city | population |
        +--------+------+------------+
        | 2      | A    | 1100       |
        +--------+------+------------+
        "#
    );

    assert_data_eq(
        harness
            .sql("SELECT offset, city, population FROM to_table(foo)")
            .await,
        expected,
    )
    .await;
    assert_data_eq(
        harness
            .sql(&format!(
                "SELECT offset, city, population FROM to_table(`{}`)",
                foo.get_id()
            ))
            .await,
        expected,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_follows_input_dataset_block_hash() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    let first_head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            schema,
        )
        .await;
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                1,A,1000
                0,A,2000
                "#
            ),
            schema,
        )
        .await;

    // The changelog and its projection must see the same state of the dataset
    let res = harness
        .sql_as_of(
            "SELECT city, population FROM to_table(foo)",
            &foo,
            first_head,
        )
        .await;

    assert_data_eq(
        res,
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            +------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_dataset_is_part_of_query_state() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    let head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // The state must pin every dataset the result depends on to be reproducible
    let state = harness
        .query_svc
        .sql_statement("SELECT * FROM to_table(foo)", QueryOptions::default())
        .await
        .unwrap()
        .state;

    assert_eq!(
        state.input_datasets,
        BTreeMap::from([(
            foo.get_id().clone(),
            QueryStateDataset {
                alias: "foo".to_string(),
                block_hash: head,
            }
        )])
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_inaccessible_dataset_is_reported_as_missing() {
    let harness =
        ToTableHarness::with_authorizer(MockDatasetActionAuthorizer::denying_ids(HashSet::from([
            ToTableHarness::dataset_id("secret"),
        ])));
    let secret = harness
        .create_dataset("secret", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    harness
        .write(
            &secret,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    // The error must not reveal that an inaccessible dataset exists
    for (statement, missing) in [
        (
            "SELECT * FROM to_table(secret)",
            "SELECT * FROM to_table(missing)",
        ),
        (
            &format!("SELECT * FROM to_table(`{}`)", secret.get_id()) as &str,
            &format!(
                "SELECT * FROM to_table(`{}`)",
                ToTableHarness::dataset_id("missing")
            ) as &str,
        ),
    ] {
        let err = harness.sql_error(statement, QueryOptions::default()).await;
        let missing_err = harness.sql_error(missing, QueryOptions::default()).await;

        assert!(err.contains("not found"), "{err}");
        assert_eq!(
            err.replace(&secret.get_id().to_string(), "<ref>")
                .replace("secret", "<ref>"),
            missing_err
                .replace(&ToTableHarness::dataset_id("missing").to_string(), "<ref>")
                .replace("missing", "<ref>"),
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_reads_only_input_datasets_of_the_query() {
    let harness = ToTableHarness::new();
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let bar = harness
        .create_dataset("bar", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    for dataset in [&foo, &bar] {
        harness
            .write(
                dataset,
                indoc!(
                    r#"
                    op,city,population
                    0,A,1000
                    "#
                ),
                schema,
            )
            .await;
    }

    // The dataset is referred to by the name the query gave it
    assert_data_eq(
        harness
            .sql_with(
                "SELECT city, population FROM to_table(renamed)",
                ToTableHarness::input_datasets(&[(&foo, "renamed")]),
            )
            .await,
        indoc!(
            r#"
            +------+------------+
            | city | population |
            +------+------------+
            | A    | 1000       |
            +------+------------+
            "#
        ),
    )
    .await;

    // Accessible datasets that are not among the inputs are not visible
    let err = harness
        .sql_error(
            "SELECT * FROM to_table(bar)",
            ToTableHarness::input_datasets(&[(&foo, "foo")]),
        )
        .await;
    assert!(err.contains("not found"), "{err}");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_dataset_without_data() {
    let harness = ToTableHarness::new();

    // No schema yet
    harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;

    // Schema, but no records
    let bar = harness
        .create_dataset("bar", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    harness
        .write(
            &bar,
            "op,city,population\n",
            "op INT NOT NULL, city STRING NOT NULL, population BIGINT",
        )
        .await;

    for name in ["foo", "bar"] {
        assert_data_eq(
            harness
                .sql(&format!("SELECT COUNT(*) AS num FROM to_table({name})"))
                .await,
            indoc!(
                r#"
                +-----+
                | num |
                +-----+
                | 0   |
                +-----+
                "#
            ),
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_joined_with_its_changelog_sees_same_block() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset("foo", ToTableHarness::changelog_stream(&["city"]), None)
        .await;
    let schema = "op INT NOT NULL, city STRING NOT NULL, population BIGINT";

    let first_head = harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                0,A,1000
                "#
            ),
            schema,
        )
        .await;
    harness
        .write(
            &foo,
            indoc!(
                r#"
                op,city,population
                1,A,1000
                0,A,2000
                "#
            ),
            schema,
        )
        .await;

    let statement = indoc!(
        r#"
        SELECT log.offset, log.op, log.population, state.population AS current
        FROM foo AS log
        JOIN to_table(foo) AS state ON log.city = state.city
        ORDER BY log.offset
        "#
    );

    assert_data_eq(
        harness.sql_as_of(statement, &foo, first_head).await,
        indoc!(
            r#"
            +--------+----+------------+---------+
            | offset | op | population | current |
            +--------+----+------------+---------+
            | 0      | 0  | 1000       | 1000    |
            +--------+----+------------+---------+
            "#
        ),
    )
    .await;

    assert_data_eq(
        harness.sql(statement).await,
        indoc!(
            r#"
            +--------+----+------------+---------+
            | offset | op | population | current |
            +--------+----+------------+---------+
            | 0      | 0  | 1000       | 2000    |
            | 1      | 1  | 1000       | 2000    |
            | 2      | 0  | 2000       | 2000    |
            +--------+----+------------+---------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test(flavor = "multi_thread"))]
async fn test_to_table_requires_primary_key() {
    let harness = ToTableHarness::new();
    let foo = harness
        .create_dataset(
            "foo",
            odf::metadata::MergeStrategy::Append(odf::metadata::MergeStrategyAppend {}),
            None,
        )
        .await;

    harness
        .write(
            &foo,
            indoc!(
                r#"
                city,population
                A,1000
                "#
            ),
            "city STRING NOT NULL, population BIGINT",
        )
        .await;

    let res = harness
        .query_svc
        .sql_statement("SELECT * FROM to_table(foo)", QueryOptions::default())
        .await;

    assert_matches!(
        res,
        Err(QueryError::Internal(e))
            if ToTableHarness::error_chain(&e).contains("'foo' has no primary keys")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct ToTableHarness {
    temp_dir: tempfile::TempDir,
    query_svc: Arc<dyn QueryService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn odf::DatasetStorageUnitWriter>,
    system_time_source: Arc<dyn SystemTimeSource>,
}

impl ToTableHarness {
    fn new() -> Self {
        Self::with_authorizer(MockDatasetActionAuthorizer::allowing())
    }

    fn with_authorizer(dataset_action_authorizer: MockDatasetActionAuthorizer) -> Self {
        let temp_dir = tempfile::tempdir().unwrap();
        let base_catalog = helpers::create_base_catalog_with_local_workspace(
            temp_dir.path(),
            dataset_action_authorizer,
        );

        let catalog = dill::CatalogBuilder::new_chained(&base_catalog)
            .add::<QueryServiceImpl>()
            .add::<kamu_auth_rebac_services::RebacDatasetRegistryFacadeImpl>()
            .build();

        Self {
            temp_dir,
            query_svc: catalog.get_one().unwrap(),
            dataset_registry: catalog.get_one().unwrap(),
            dataset_storage_unit_writer: catalog.get_one().unwrap(),
            system_time_source: catalog.get_one().unwrap(),
        }
    }

    /// Datasets get IDs derived from their names, so they can be referred to before creation
    fn dataset_id(name: &str) -> odf::DatasetID {
        odf::DatasetID::new_seeded_ed25519(name.as_bytes())
    }

    fn changelog_stream(primary_key: &[&str]) -> odf::metadata::MergeStrategy {
        odf::metadata::MergeStrategy::ChangelogStream(odf::metadata::MergeStrategyChangelogStream {
            primary_key: primary_key.iter().map(ToString::to_string).collect(),
        })
    }

    /// Dataset with a polling source using the specified merge strategy
    async fn create_dataset(
        &self,
        name: &str,
        merge: odf::metadata::MergeStrategy,
        vocab: Option<odf::metadata::SetVocab>,
    ) -> ResolvedDataset {
        self.create_dataset_with_source(
            name,
            MetadataFactory::set_polling_source()
                .merge(merge)
                .build()
                .into(),
            vocab,
        )
        .await
    }

    async fn create_dataset_with_source(
        &self,
        name: &str,
        source: odf::MetadataEvent,
        vocab: Option<odf::metadata::SetVocab>,
    ) -> ResolvedDataset {
        let alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked(name));

        let mut snapshot = MetadataFactory::dataset_snapshot()
            .name(alias.clone())
            .kind(odf::DatasetKind::Root);
        if let Some(vocab) = vocab {
            snapshot = snapshot.push_event(vocab);
        }
        let snapshot = snapshot.push_event(source).build();

        let stored = create_test_dataset_from_snapshot(
            self.dataset_registry.as_ref(),
            self.dataset_storage_unit_writer.as_ref(),
            snapshot,
            Self::dataset_id(name),
            self.system_time_source.now(),
        )
        .await
        .unwrap();

        ResolvedDataset::from_stored(&stored, &alias)
    }

    /// Ingests CSV data through the data writer and returns the new head
    async fn write(&self, dataset: &ResolvedDataset, csv: &str, schema: &str) -> odf::Multihash {
        // Natural order of records is only preserved in a single partition
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));

        let data_path = self.temp_dir.path().join("data.csv");
        std::fs::write(&data_path, csv).unwrap();

        let df = ReaderCsv::new(
            ctx.clone(),
            odf::metadata::ReadStepCsv {
                header: Some(true),
                schema: Some(odf::utils::schema::parse::parse_ddl_to_odf_schema(schema).unwrap()),
                ..Default::default()
            },
            &odf::schema::ToArrowSettings::default(),
        )
        .unwrap()
        .read(&data_path)
        .await
        .unwrap();

        let mut writer = DataWriterDataFusion::from_metadata_chain(
            ctx,
            dataset.clone(),
            &odf::BlockRef::Head,
            None,
        )
        .await
        .unwrap();

        let now = self.system_time_source.now();
        let write_result = writer
            .write(
                Some(df),
                WriteDataOpts {
                    system_time: now,
                    source_event_time: now,
                    new_watermark: None,
                    new_source_state: None,
                    data_staging_path: self.temp_dir.path().join(".temp-data.parquet"),
                },
            )
            .await
            .unwrap();

        dataset
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                &write_result.new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(Some(&write_result.old_head)),
                },
            )
            .await
            .unwrap();

        write_result.new_head
    }

    /// Messages of the error and all its sources, as wrapping layers hide the cause
    fn error_chain(err: &dyn std::error::Error) -> String {
        std::iter::successors(Some(err), |e| e.source())
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(": ")
    }

    async fn sql(&self, statement: &str) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(statement, QueryOptions::default())
            .await
            .unwrap()
            .df
    }

    /// Error messages of a failing statement, including all their sources
    async fn sql_error(&self, statement: &str, options: QueryOptions) -> String {
        let Err(err) = self.query_svc.sql_statement(statement, options).await else {
            panic!("Expected statement to fail: {statement}");
        };
        Self::error_chain(&err)
    }

    /// Options that restrict the query to the specified datasets under the specified names
    fn input_datasets(datasets: &[(&ResolvedDataset, &str)]) -> QueryOptions {
        QueryOptions {
            input_datasets: Some(
                datasets
                    .iter()
                    .map(|(dataset, alias)| {
                        (
                            dataset.get_id().clone(),
                            QueryOptionsDataset {
                                alias: (*alias).to_string(),
                                ..Default::default()
                            },
                        )
                    })
                    .collect(),
            ),
        }
    }

    async fn sql_with(
        &self,
        statement: &str,
        options: QueryOptions,
    ) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(statement, options)
            .await
            .unwrap()
            .df
    }

    /// Runs the statement with the dataset pinned to the specified block
    async fn sql_as_of(
        &self,
        statement: &str,
        dataset: &ResolvedDataset,
        block_hash: odf::Multihash,
    ) -> odf::utils::data::DataFrameExt {
        self.query_svc
            .sql_statement(
                statement,
                QueryOptions {
                    input_datasets: Some(BTreeMap::from([(
                        dataset.get_id().clone(),
                        QueryOptionsDataset {
                            alias: dataset.get_alias().dataset_name.to_string(),
                            block_hash: Some(block_hash),
                            ..Default::default()
                        },
                    )])),
                },
            )
            .await
            .unwrap()
            .df
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
