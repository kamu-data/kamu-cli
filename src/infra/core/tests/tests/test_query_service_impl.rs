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
use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use dill::{Catalog, Component};
use event_bus::EventBus;
use kamu::domain::*;
use kamu::testing::{
    LocalS3Server,
    MetadataFactory,
    MockDatasetActionAuthorizer,
    ParquetWriterHelper,
};
use kamu::utils::s3_context::S3Context;
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_ingest_datafusion::DataWriterDataFusion;
use opendatafabric::*;
use tempfile::TempDir;

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(catalog: &dill::Catalog, tempdir: &Path) -> CreateDatasetResult {
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let create_result = dataset_repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();

    let dataset = create_result.dataset.clone();

    // Write schema
    let tmp_data_path = tempdir.join("data");
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

    // Write data spread over two commits
    let batches = [
        (
            UInt64Array::from(vec![0, 1]),
            StringArray::from(vec!["a", "b"]),
        ),
        (
            UInt64Array::from(vec![2, 3]),
            StringArray::from(vec!["c", "d"]),
        ),
    ];

    // TODO: Replace with DataWriter
    let mut prev_offset = None;
    for (a, b) in batches {
        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();
        ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

        let start_offset = prev_offset.map_or(0, |v| v + 1);
        let end_offset = start_offset + record_batch.num_rows() as u64 - 1;

        dataset
            .commit_add_data(
                AddDataParams {
                    prev_checkpoint: None,
                    prev_offset,
                    new_offset_interval: Some(OffsetInterval {
                        start: start_offset,
                        end: end_offset,
                    }),
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(tmp_data_path.clone())),
                None,
                CommitOpts::default(),
            )
            .await
            .unwrap();

        prev_offset = Some(end_offset);
    }

    create_result
}

/////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog_with_local_workspace(
    tempdir: &Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryLocalFs::builder()
                .with_root(datasets_dir)
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<ObjectStoreBuilderLocalFs>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_action_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_s3_workspace(
    s3: &LocalS3Server,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let (endpoint, bucket, key_prefix) = S3Context::split_url(&s3.url);
    let s3_context = S3Context::from_items(endpoint.clone(), bucket, key_prefix).await;

    dill::CatalogBuilder::new()
        .add::<SystemTimeSourceDefault>()
        .add::<EventBus>()
        .add::<DependencyGraphServiceInMemory>()
        .add_builder(
            DatasetRepositoryS3::builder()
                .with_s3_context(s3_context.clone())
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
        .add::<QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<ObjectStoreBuilderLocalFs>()
        .add_value(ObjectStoreBuilderS3::new(s3_context, true))
        .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderS3>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_action_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_parquet_schema(catalog: &Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema = query_svc
        .get_schema_parquet_file(&dataset_ref)
        .await
        .unwrap();
    assert!(schema.is_some());

    let mut buf = Vec::new();
    kamu_data_utils::schema::format::write_schema_parquet_json(&mut buf, &schema.unwrap()).unwrap();
    let schema_content = String::from_utf8(buf).unwrap();
    let data_schema_json =
        serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

    assert_eq!(
        data_schema_json,
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
async fn test_dataset_arrow_schema(catalog: &Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema_ref = query_svc.get_schema(&dataset_ref).await.unwrap().unwrap();

    let mut buf = Vec::new();
    kamu_data_utils::schema::format::write_schema_arrow_json(&mut buf, schema_ref.as_ref())
        .unwrap();

    let schema_content = String::from_utf8(buf).unwrap();
    let data_schema_json =
        serde_json::from_str::<serde_json::Value>(schema_content.as_str()).unwrap();

    assert_eq!(
        data_schema_json,
        serde_json::json!({
            "fields": [
                {
                    "name": "offset",
                    "data_type": "UInt64",
                    "nullable": false,
                    "dict_id": 0,
                    "dict_is_ordered": false,
                    "metadata": {}
                },
                {
                    "name": "blah",
                    "data_type": "Utf8",
                    "nullable": false,
                    "dict_id": 0,
                    "dict_is_ordered": false,
                    "metadata": {}
                }
            ],
            "metadata": {}
        })
    );
}

fn prepare_test_catalog() -> (TempDir, Catalog) {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1),
    );
    (tempdir, catalog)
}

async fn prepare_test_s3_catalog() -> (LocalS3Server, Catalog) {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1),
    )
    .await;
    (s3, catalog)
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_local_fs() {
    let (tempdir, catalog) = prepare_test_catalog();
    test_dataset_parquet_schema(&catalog, &tempdir).await;
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_arrow_schema_local_fs() {
    let (tempdir, catalog) = prepare_test_catalog();
    test_dataset_arrow_schema(&catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_s3() {
    let (s3, catalog) = prepare_test_s3_catalog().await;
    test_dataset_parquet_schema(&catalog, &s3.tmp_dir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_arrow_schema_s3() {
    let (s3, catalog) = prepare_test_s3_catalog().await;
    test_dataset_arrow_schema(&catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let result = query_svc.get_schema(&dataset_ref).await;
    assert_matches!(result, Err(QueryError::Access(_)));
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_unauthorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::denying());
    test_dataset_schema_unauthorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_unauthorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog =
        create_catalog_with_s3_workspace(&s3, MockDatasetActionAuthorizer::denying()).await;
    test_dataset_schema_unauthorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = DatasetRef::from(dataset_alias);

    // Within last block
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let df = query_svc.tail(&dataset_ref, 1, 1).await.unwrap();

    kamu_data_utils::testing::assert_data_eq(
        df,
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
    let df = query_svc.tail(&dataset_ref, 1, 2).await.unwrap();

    kamu_data_utils::testing::assert_data_eq(
        df,
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
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(4),
    );
    test_dataset_tail_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(4),
    )
    .await;
    test_dataset_tail_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1),
    );

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    dataset_repo
        .create_dataset_from_seed(
            &"foo".try_into().unwrap(),
            MetadataFactory::seed(DatasetKind::Root).build(),
        )
        .await
        .unwrap();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc.tail(&"foo".try_into().unwrap(), 0, 10).await;
    assert_matches!(res, Err(QueryError::DatasetSchemaNotAvailable(_)));
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let result = query_svc.tail(&dataset_alias.as_local_ref(), 1, 1).await;
    assert_matches!(result, Err(QueryError::Access(_)));
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_unauthorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::denying());
    test_dataset_tail_unauthorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_unauthorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog =
        create_catalog_with_s3_workspace(&s3, MockDatasetActionAuthorizer::denying()).await;
    test_dataset_tail_unauthorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_sql_authorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = format!("SELECT COUNT(*) AS num_records FROM {dataset_alias}");
    let res = query_svc
        .sql_statement(statement.as_str(), QueryOptions::default())
        .await
        .unwrap();

    kamu_data_utils::testing::assert_data_eq(
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
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1),
    );
    test_dataset_sql_authorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_authorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1),
    )
    .await;
    test_dataset_sql_authorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_sql_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = format!("SELECT COUNT(*) FROM {dataset_alias}");
    let result = query_svc
        .sql_statement(statement.as_str(), QueryOptions::default())
        .await;

    assert_matches!(
        result,
        Err(QueryError::DataFusionError(
            datafusion::common::DataFusionError::Plan(s)
        ))  if s.contains("table 'kamu.kamu.foo' not found")
    );
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_unauthorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog =
        create_catalog_with_local_workspace(tempdir.path(), MockDatasetActionAuthorizer::denying());
    test_dataset_sql_unauthorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_sql_unauthorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog =
        create_catalog_with_s3_workspace(&s3, MockDatasetActionAuthorizer::denying()).await;
    test_dataset_sql_unauthorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let _ = create_test_dataset(&catalog, tempdir.path()).await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) from does_not_exist";
    let result = query_svc
        .sql_statement(statement, QueryOptions::default())
        .await;

    assert_matches!(
        result,
        Err(QueryError::DataFusionError(
            ::datafusion::common::DataFusionError::Plan(s)
        ))  if s.contains("table 'kamu.kamu.does_not_exist' not found")
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_by_alias() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let dataset = create_test_dataset(&catalog, tempdir.path()).await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) as num_records from foobar";
    let result = query_svc
        .sql_statement(
            statement,
            QueryOptions {
                aliases: Some(BTreeMap::from([(
                    "foobar".to_string(),
                    dataset.dataset_handle.id,
                )])),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    kamu_data_utils::testing::assert_data_eq(
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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_alias_not_found() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();

    // Note that we use an alias on top of existing dataset name - alias must take
    // precedence
    let statement = format!("select count(*) as num_records from {dataset_alias}");
    let result = query_svc
        .sql_statement(
            statement.as_str(),
            QueryOptions {
                aliases: Some(BTreeMap::from([(
                    dataset_alias.to_string(),
                    DatasetID::new_seeded_ed25519(b"does-not-exist"),
                )])),
                ..Default::default()
            },
        )
        .await;

    assert_matches!(
        result,
        Err(QueryError::DatasetNotFound(DatasetNotFoundError {
            dataset_ref,
        })) if dataset_ref == DatasetID::new_seeded_ed25519(b"does-not-exist").as_local_ref()
    );
}

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_with_state_simple() {
    use ::datafusion::prelude::*;

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let ctx = SessionContext::new();

    // Dataset init
    let foo_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let foo_create = dataset_repo
        .create_dataset(
            &foo_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();
    let foo_id = foo_create.dataset_handle.id;
    let foo_dataset = foo_create.dataset;

    let mut writer = DataWriterDataFusion::builder(foo_dataset.clone(), ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap()
        .build();

    writer
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

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

    kamu_data_utils::testing::assert_data_eq(
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
        res.state.inputs,
        BTreeMap::from([(
            foo_id.clone(),
            foo_dataset
                .as_metadata_chain()
                .resolve_ref(&BlockRef::Head)
                .await
                .unwrap()
        )])
    );

    // Add more data
    writer
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

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
                as_of_state: Some(res.state),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    kamu_data_utils::testing::assert_data_eq(
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

    kamu_data_utils::testing::assert_data_eq(
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

/////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_with_state_cte() {
    use ::datafusion::prelude::*;

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::allowing(),
    );

    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let ctx = SessionContext::new();

    // Dataset `foo`
    let foo_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));
    let foo_create = dataset_repo
        .create_dataset(
            &foo_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Root).id_random().build(),
            )
            .build_typed(),
        )
        .await
        .unwrap();
    let foo_id = foo_create.dataset_handle.id;
    let foo_dataset = foo_create.dataset;

    let mut writer_foo = DataWriterDataFusion::builder(foo_dataset.clone(), ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap()
        .build();

    writer_foo
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

    // Dataset `bar`
    let bar_alias = DatasetAlias::new(None, DatasetName::new_unchecked("bar"));
    let bar_create = dataset_repo
        .create_dataset(
            &bar_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(DatasetKind::Root).id_random().build(),
            )
            .build_typed(),
        )
        .await
        .unwrap();
    let bar_id = bar_create.dataset_handle.id;
    let bar_dataset = bar_create.dataset;

    let mut writer_bar = DataWriterDataFusion::builder(bar_dataset.clone(), ctx.clone())
        .with_metadata_state_scanned(None)
        .await
        .unwrap()
        .build();

    writer_bar
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

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

    kamu_data_utils::testing::assert_data_eq(
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
        res.state.inputs,
        BTreeMap::from([
            (
                foo_id.clone(),
                foo_dataset
                    .as_metadata_chain()
                    .resolve_ref(&BlockRef::Head)
                    .await
                    .unwrap()
            ),
            (
                bar_id.clone(),
                bar_dataset
                    .as_metadata_chain()
                    .resolve_ref(&BlockRef::Head)
                    .await
                    .unwrap()
            )
        ])
    );

    // Add more data
    writer_foo
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

    writer_bar
        .write(
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
                .unwrap(),
            ),
            WriteDataOpts {
                system_time: Utc::now(),
                source_event_time: Utc::now(),
                new_watermark: None,
                new_source_state: None,
                data_staging_path: tempdir.path().join(".temp-data"),
            },
        )
        .await
        .unwrap();

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

    kamu_data_utils::testing::assert_data_eq(
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
                as_of_state: Some(res.state.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    kamu_data_utils::testing::assert_data_eq(
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
                aliases: Some(BTreeMap::from([
                    ("fooz".to_string(), foo_id.clone()),
                    ("barz".to_string(), bar_id.clone()),
                ])),
                as_of_state: Some(res.state),
                ..Default::default()
            },
        )
        .await
        .unwrap();

    kamu_data_utils::testing::assert_data_eq(
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

/////////////////////////////////////////////////////////////////////////////////////////
