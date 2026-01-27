// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use kamu::domain::*;
use kamu::*;
use kamu_datasets::ResolvedDataset;
use kamu_datasets_services::testing::MockDatasetActionAuthorizer;
use kamu_ingest_datafusion::DataWriterDataFusion;
use odf::utils::data::DataFrameExt;
use tempfile::TempDir;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceStub};

use crate::tests::queries::helpers;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog_with_local_workspace(
    tempdir: &Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_local_catalog =
        helpers::create_base_catalog_with_local_workspace(tempdir, dataset_action_authorizer);

    dill::CatalogBuilder::new_chained(&base_local_catalog)
        .add::<QueryServiceImpl>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_s3_workspace(
    s3: &LocalS3Server,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let base_s3_catalog =
        helpers::create_base_catalog_with_s3_workspace(s3, dataset_action_authorizer).await;

    dill::CatalogBuilder::new_chained(&base_s3_catalog)
        .add::<QueryServiceImpl>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;

    // Within last block
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .tail(target.clone(), 1, 1, GetDataOptions::default())
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df.unwrap(),
        indoc::indoc!(
            r#"
            +--------+------+
            | offset | blah |
            +--------+------+
            | 2      | c    |
            +--------+------+
            "#
        ),
    )
    .await;

    // Crosses block boundary
    let res = query_svc
        .tail(target, 1, 2, GetDataOptions::default())
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df.unwrap(),
        indoc::indoc!(
            r#"
            +--------+------+
            | offset | blah |
            +--------+------+
            | 1      | b    |
            | 2      | c    |
            +--------+------+
            "#
        ),
    )
    .await;
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );
    test_dataset_tail_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog =
        create_catalog_with_s3_workspace(&s3, MockDatasetActionAuthorizer::allowing()).await;
    test_dataset_tail_common(catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::new());
    let (created, dataset_alias) = helpers::create_empty_dataset(&catalog, "foo").await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .tail(
            ResolvedDataset::from_stored(&created, &dataset_alias),
            0,
            10,
            GetDataOptions::default(),
        )
        .await
        .unwrap();
    assert_matches!(res.df, None);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_sql_authorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;
    let dataset_alias = target.get_alias();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = format!("SELECT COUNT(*) AS num_records FROM {dataset_alias}");
    let res = query_svc
        .sql_statement(statement.as_str(), QueryOptions::default())
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +-------------+
            | num_records |
            +-------------+
            | 4           |
            +-------------+
            "#
        ),
    )
    .await;
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_authorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1, true),
    );
    test_dataset_sql_authorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_authorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1, true),
    )
    .await;
    test_dataset_sql_authorized_common(catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_sql_unauthorized_infer_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;
    let dataset_alias = target.get_alias();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = format!("SELECT COUNT(*) FROM {dataset_alias}");
    let result = query_svc
        .sql_statement(statement.as_str(), QueryOptions::default())
        .await;

    assert_matches!(
        result,
        Err(QueryError::BadQuery(e)) if e.to_string().contains("table 'kamu.kamu.foo' not found")
    );
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_unauthorized_infer_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::denying());
    test_dataset_sql_unauthorized_infer_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_unauthorized_infer_local_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog =
        create_catalog_with_s3_workspace(&s3, MockDatasetActionAuthorizer::denying()).await;
    test_dataset_sql_unauthorized_infer_common(catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_unauthorized_specific() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::denying());

    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;
    let dataset_alias = target.get_alias();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = format!("SELECT COUNT(*) FROM {dataset_alias}");
    let result = query_svc
        .sql_statement(
            statement.as_str(),
            QueryOptions {
                input_datasets: Some(BTreeMap::from([(
                    target.get_id().clone(),
                    QueryOptionsDataset {
                        alias: "foo".to_string(),
                        ..Default::default()
                    },
                )])),
            },
        )
        .await;

    assert_matches!(
        result,
        Err(QueryError::BadQuery(e)) if e.to_string().contains("table 'kamu.kamu.foo' not found")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let _ = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) from does_not_exist";
    let result = query_svc
        .sql_statement(statement, QueryOptions::default())
        .await;

    assert_matches!(
        result,
        Err(QueryError::BadQuery(e))
        if e.to_string().contains("table 'kamu.kamu.does_not_exist' not found")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_by_alias() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) as num_records from foobar";
    let result = query_svc
        .sql_statement(
            statement,
            QueryOptions {
                input_datasets: Some(BTreeMap::from([(
                    target.get_id().clone(),
                    QueryOptionsDataset {
                        alias: "foobar".to_string(),
                        ..Default::default()
                    },
                )])),
            },
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        result.df,
        indoc::indoc!(
            r#"
            +-------------+
            | num_records |
            +-------------+
            | 4           |
            +-------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_alias_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let target = helpers::create_test_dataset(&catalog, tempdir.path(), "foo").await;
    let dataset_alias = target.get_alias();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    // Note that we use an alias on top of existing dataset name - alias must take
    // precedence
    let statement = format!("select count(*) as num_records from {dataset_alias}");
    let result = query_svc
        .sql_statement(
            statement.as_str(),
            QueryOptions {
                input_datasets: Some(BTreeMap::from([(
                    odf::DatasetID::new_seeded_ed25519(b"does-not-exist"),
                    QueryOptionsDataset {
                        alias: dataset_alias.to_string(),
                        ..Default::default()
                    },
                )])),
            },
        )
        .await;

    assert_matches!(
        result,
        Err(QueryError::DatasetNotFound(odf::DatasetNotFoundError {
            dataset_ref,
        })) if dataset_ref == odf::DatasetID::new_seeded_ed25519(b"does-not-exist").as_local_ref()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_with_state_simple() {
    use ::datafusion::prelude::*;

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let ctx = SessionContext::new();

    // Dataset init
    let (foo_stored, foo_alias) = helpers::create_empty_dataset(&catalog, "foo").await;
    let foo_id = &foo_stored.dataset_id;

    let foo_target = ResolvedDataset::from_stored(&foo_stored, &foo_alias);

    let mut writer = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        foo_target.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    write_data(
        &catalog,
        foo_target.clone(),
        &mut writer,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(UInt64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // Query: initial
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                select
                    cat,
                    sum(num) as sum
                from {foo_alias}
                group by cat
                order by 1
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 1   |
            | b   | 2   |
            +-----+-----+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.state.input_datasets,
        BTreeMap::from([(
            foo_id.clone(),
            QueryStateDataset {
                alias: "foo".to_string(),
                block_hash: foo_stored
                    .dataset
                    .as_metadata_chain()
                    .resolve_ref(&odf::BlockRef::Head)
                    .await
                    .unwrap()
            }
        )])
    );

    // Add more data
    write_data(
        &catalog,
        foo_target,
        &mut writer,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(UInt64Array::from(vec![2, 4])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // Query: again with previous state info
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                select
                    cat,
                    sum(num) as sum
                from {foo_alias}
                group by cat
                order by 1
                "#
            ),
            QueryOptions {
                input_datasets: Some(
                    res.state
                        .input_datasets
                        .into_iter()
                        .map(|(id, s)| {
                            (
                                id,
                                QueryOptionsDataset {
                                    alias: s.alias,
                                    block_hash: Some(s.block_hash),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                ),
            },
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 1   |
            | b   | 2   |
            +-----+-----+
            "#
        ),
    )
    .await;

    // Query: again without state info
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                select
                    cat,
                    sum(num) as sum
                from {foo_alias}
                group by cat
                order by 1
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 3   |
            | b   | 6   |
            +-----+-----+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_with_state_cte() {
    use ::datafusion::prelude::*;

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let ctx = SessionContext::new();

    // Dataset `foo`
    let (foo_stored, foo_alias) = helpers::create_empty_dataset(&catalog, "foo").await;
    let foo_id = &foo_stored.dataset_id;

    let foo_target = ResolvedDataset::from_stored(&foo_stored, &foo_alias);

    let mut writer_foo = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        foo_target.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    write_data(
        &catalog,
        foo_target.clone(),
        &mut writer_foo,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(UInt64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // Dataset `bar`
    let (bar_stored, bar_alias) = helpers::create_empty_dataset(&catalog, "bar").await;
    let bar_id = &bar_stored.dataset_id;

    let bar_target = ResolvedDataset::from_stored(&bar_stored, &bar_alias);

    let mut writer_bar = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        bar_target.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    write_data(
        &catalog,
        bar_target.clone(),
        &mut writer_bar,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["b", "c"])),
                        Arc::new(UInt64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // Query: initial
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                with concat as (
                    select * from {foo_alias}
                    union all
                    select * from {bar_alias}
                )
                select
                    cat,
                    sum(num) as sum
                from concat
                group by cat
                order by 1
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 1   |
            | b   | 3   |
            | c   | 2   |
            +-----+-----+
            "#
        ),
    )
    .await;

    assert_eq!(
        res.state.input_datasets,
        BTreeMap::from([
            (
                foo_id.clone(),
                QueryStateDataset {
                    alias: "foo".to_string(),
                    block_hash: foo_stored
                        .dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .unwrap()
                }
            ),
            (
                bar_id.clone(),
                QueryStateDataset {
                    alias: "bar".to_string(),
                    block_hash: bar_stored
                        .dataset
                        .as_metadata_chain()
                        .resolve_ref(&odf::BlockRef::Head)
                        .await
                        .unwrap()
                }
            ),
        ])
    );

    // Add more data
    write_data(
        &catalog,
        foo_target,
        &mut writer_foo,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(UInt64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    write_data(
        &catalog,
        bar_target,
        &mut writer_bar,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("cat", DataType::Utf8, false),
                        Field::new("num", DataType::UInt64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["b", "c"])),
                        Arc::new(UInt64Array::from(vec![1, 2])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // Query: new data
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res2 = query_svc
        .sql_statement(
            &format!(
                r#"
                with concat as (
                    select * from {foo_alias}
                    union all
                    select * from {bar_alias}
                )
                select
                    cat,
                    sum(num) as sum
                from concat
                group by cat
                order by 1
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res2.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 2   |
            | b   | 6   |
            | c   | 4   |
            +-----+-----+
            "#
        ),
    )
    .await;

    // Query: new data again but with original state
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res2 = query_svc
        .sql_statement(
            &format!(
                r#"
                with concat as (
                    select * from {foo_alias}
                    union all
                    select * from {bar_alias}
                )
                select
                    cat,
                    sum(num) as sum
                from concat
                group by cat
                order by 1
                "#
            ),
            QueryOptions {
                input_datasets: Some(
                    res.state
                        .input_datasets
                        .iter()
                        .map(|(id, s)| {
                            (
                                id.clone(),
                                QueryOptionsDataset {
                                    alias: s.alias.clone(),
                                    block_hash: Some(s.block_hash.clone()),
                                    ..Default::default()
                                },
                            )
                        })
                        .collect(),
                ),
            },
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res2.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 1   |
            | b   | 3   |
            | c   | 2   |
            +-----+-----+
            "#
        ),
    )
    .await;

    // Query: query with prev state again, but now also with aliases
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res2 = query_svc
        .sql_statement(
            r#"
            with concat as (
                select * from fooz
                union all
                select * from barz
            )
            select
                cat,
                sum(num) as sum
            from concat
            group by cat
            order by 1
            "#,
            QueryOptions {
                input_datasets: Some(BTreeMap::from([
                    (
                        foo_id.clone(),
                        QueryOptionsDataset {
                            alias: "fooz".to_string(),
                            block_hash: Some(
                                res.state
                                    .input_datasets
                                    .get(foo_id)
                                    .unwrap()
                                    .block_hash
                                    .clone(),
                            ),
                            ..Default::default()
                        },
                    ),
                    (
                        bar_id.clone(),
                        QueryOptionsDataset {
                            alias: "barz".to_string(),
                            block_hash: Some(
                                res.state
                                    .input_datasets
                                    .get(bar_id)
                                    .unwrap()
                                    .block_hash
                                    .clone(),
                            ),
                            ..Default::default()
                        },
                    ),
                ])),
            },
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res2.df,
        indoc::indoc!(
            r#"
            +-----+-----+
            | cat | sum |
            +-----+-----+
            | a   | 1   |
            | b   | 3   |
            | c   | 2   |
            +-----+-----+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_with_schema_migration() {
    use ::datafusion::arrow::datatypes::DataType;
    use ::datafusion::prelude::*;
    use odf::schema::*;

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let time_source = catalog.get_one::<SystemTimeSourceStub>().unwrap();
    time_source.set(Utc.with_ymd_and_hms(2010, 1, 1, 12, 0, 0).unwrap());

    let ctx = SessionContext::new();

    // 0: Init dataset
    let (foo_stored, foo_alias) = helpers::create_empty_dataset(&catalog, "foo").await;

    let foo_target = ResolvedDataset::from_stored(&foo_stored, &foo_alias);

    foo_target
        .commit_event(
            odf::metadata::SetDataSchema::new(DataSchema::new(vec![
                DataField::i64("offset"),
                DataField::i32("op"),
                DataField::timestamp_millis_utc("system_time"),
                DataField::timestamp_millis_utc("event_time"),
                DataField::string("city"),
                DataField::i64("population"),
            ]))
            .into(),
            odf::dataset::CommitOpts {
                system_time: Some(time_source.now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let mut writer_foo = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        foo_target.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    // 1: Add data
    write_data(
        &catalog,
        foo_target.clone(),
        &mut writer_foo,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("city", DataType::Utf8, false),
                        Field::new("population", DataType::Int64, false),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["a", "b"])),
                        Arc::new(Int64Array::from(vec![100, 200])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // 2: Query
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                select * from {foo_alias}
                order by offset
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | a    | 100        |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | b    | 200        |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;

    // 3: Add new `census_url` optional field
    foo_target
        .commit_event(
            odf::metadata::SetDataSchema::new(DataSchema::new(vec![
                DataField::i64("offset"),
                DataField::i32("op"),
                DataField::timestamp_millis_utc("system_time"),
                DataField::timestamp_millis_utc("event_time"),
                DataField::string("city"),
                DataField::i64("population"),
                DataField::string("census_url").optional(),
            ]))
            .into(),
            odf::dataset::CommitOpts {
                system_time: Some(time_source.now()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // Need to re-initialize the writer to pick up the new schema
    let mut writer_foo = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        foo_target.clone(),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

    // 4: Add data with new schema
    write_data(
        &catalog,
        foo_target.clone(),
        &mut writer_foo,
        Some(
            ctx.read_batch(
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("city", DataType::Utf8, false),
                        Field::new("population", DataType::Int64, false),
                        Field::new("census_url", DataType::Utf8, true),
                    ])),
                    vec![
                        Arc::new(StringArray::from(vec!["c", "d"])),
                        Arc::new(Int64Array::from(vec![300, 400])),
                        Arc::new(StringArray::from(vec![Some("http://c.ca/census"), None])),
                    ],
                )
                .unwrap(),
            )
            .unwrap()
            .into(),
        ),
        tempdir.path().join(".temp-data.parquet"),
    )
    .await;

    // 5: Query
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc
        .sql_statement(
            &format!(
                r#"
                select * from {foo_alias}
                order by offset
                "#
            ),
            QueryOptions::default(),
        )
        .await
        .unwrap();

    odf::utils::testing::assert_data_eq(
        res.df,
        indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+--------------------+
            | offset | op | system_time          | event_time           | city | population | census_url         |
            +--------+----+----------------------+----------------------+------+------------+--------------------+
            | 0      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | a    | 100        |                    |
            | 1      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | b    | 200        |                    |
            | 2      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | c    | 300        | http://c.ca/census |
            | 3      | 0  | 2010-01-01T12:00:00Z | 2010-01-01T12:00:00Z | d    | 400        |                    |
            +--------+----+----------------------+----------------------+------+------------+--------------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn write_data(
    catalog: &dill::Catalog,
    target: ResolvedDataset,
    writer: &mut DataWriterDataFusion,
    data: Option<DataFrameExt>,
    data_staging_path: PathBuf,
) {
    let time = catalog.get_one::<dyn SystemTimeSource>().unwrap();
    let now = time.now();

    let write_result = writer
        .write(
            data,
            WriteDataOpts {
                system_time: now,
                source_event_time: now,
                new_watermark: None,
                new_source_state: None,
                data_staging_path,
            },
        )
        .await
        .unwrap();

    target
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
