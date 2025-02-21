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
use file_utils::OwnedFile;
use kamu::domain::*;
use kamu::testing::{MockDatasetActionAuthorizer, ParquetWriterHelper};
use kamu::*;
use kamu_accounts::CurrentAccountSubject;
use kamu_ingest_datafusion::DataWriterDataFusion;
use odf::metadata::testing::MetadataFactory;
use s3_utils::S3Context;
use tempfile::TempDir;
use test_utils::LocalS3Server;
use time_source::{SystemTimeSource, SystemTimeSourceDefault};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(catalog: &dill::Catalog, tempdir: &Path) -> odf::CreateDatasetResult {
    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();
    let dataset_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));

    let create_result = dataset_storage_unit_writer
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
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
            odf::dataset::CommitOpts::default(),
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
                odf::dataset::AddDataParams {
                    prev_checkpoint: None,
                    prev_offset,
                    new_offset_interval: Some(odf::metadata::OffsetInterval {
                        start: start_offset,
                        end: end_offset,
                    }),
                    new_watermark: None,
                    new_source_state: None,
                },
                Some(OwnedFile::new(tmp_data_path.clone())),
                None,
                odf::dataset::CommitOpts::default(),
            )
            .await
            .unwrap();

        prev_offset = Some(end_offset);
    }

    create_result
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn create_catalog_with_local_workspace(
    tempdir: &Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let datasets_dir = tempdir.join("datasets");
    std::fs::create_dir(&datasets_dir).unwrap();

    dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitLocalFs::builder().with_root(datasets_dir))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitLocalFs>()
        .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitLocalFs>()
        .add::<DatasetRegistrySoloUnitBridge>()
        .add::<QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .add::<ObjectStoreBuilderLocalFs>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_action_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .build()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_s3_workspace(
    s3: &LocalS3Server,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    let s3_context = S3Context::from_url(&s3.url).await;

    dill::CatalogBuilder::new()
        .add::<DidGeneratorDefault>()
        .add::<SystemTimeSourceDefault>()
        .add_value(TenancyConfig::SingleTenant)
        .add_builder(DatasetStorageUnitS3::builder().with_s3_context(s3_context.clone()))
        .bind::<dyn odf::DatasetStorageUnit, DatasetStorageUnitS3>()
        .bind::<dyn odf::DatasetStorageUnitWriter, DatasetStorageUnitS3>()
        .add::<DatasetRegistrySoloUnitBridge>()
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_parquet_schema(catalog: &Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = odf::DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema = query_svc
        .get_schema_parquet_file(&dataset_ref)
        .await
        .unwrap();
    assert!(schema.is_some());

    let mut buf = Vec::new();
    odf::utils::schema::format::write_schema_parquet_json(&mut buf, &schema.unwrap()).unwrap();
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
    let dataset_ref = odf::DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema_ref = query_svc.get_schema(&dataset_ref).await.unwrap().unwrap();

    let mut buf = Vec::new();
    odf::utils::schema::format::write_schema_arrow_json(&mut buf, schema_ref.as_ref()).unwrap();

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

fn prepare_schema_test_catalog() -> (TempDir, Catalog) {
    let mut authorizer = MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1, true);
    authorizer
        .expect_filter_datasets_allowing()
        .returning(|_, _| Ok(vec![]));

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), authorizer);
    (tempdir, catalog)
}

async fn prepare_schema_test_s3_catalog() -> (LocalS3Server, Catalog) {
    let mut authorizer = MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(1, true);
    authorizer
        .expect_filter_datasets_allowing()
        .returning(|_, _| Ok(vec![]));

    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(&s3, authorizer).await;
    (s3, catalog)
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_local_fs() {
    let (tempdir, catalog) = prepare_schema_test_catalog();
    test_dataset_parquet_schema(&catalog, &tempdir).await;
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_arrow_schema_local_fs() {
    let (tempdir, catalog) = prepare_schema_test_catalog();
    test_dataset_arrow_schema(&catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_parquet_schema_s3() {
    let (s3, catalog) = prepare_schema_test_s3_catalog().await;
    test_dataset_parquet_schema(&catalog, &s3.tmp_dir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_arrow_schema_s3() {
    let (s3, catalog) = prepare_schema_test_s3_catalog().await;
    test_dataset_arrow_schema(&catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = odf::DatasetRef::from(dataset_alias);

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path())
        .await
        .dataset_handle
        .alias;
    let dataset_ref = odf::DatasetRef::from(dataset_alias);

    // Within last block
    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let df = query_svc.tail(&dataset_ref, 1, 1).await.unwrap();

    odf::utils::testing::assert_data_eq(
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

    odf::utils::testing::assert_data_eq(
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
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(4, true),
    );
    test_dataset_tail_common(catalog, &tempdir).await;
}

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(4, true),
    )
    .await;
    test_dataset_tail_common(catalog, &s3.tmp_dir).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_dataset_tail_empty_dataset() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        MockDatasetActionAuthorizer::new().expect_check_read_a_dataset(2, true),
    );

    let did_generator = catalog.get_one::<dyn DidGenerator>().unwrap();
    let time_source = catalog.get_one::<dyn SystemTimeSource>().unwrap();
    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();

    dataset_storage_unit_writer
        .create_dataset(
            &"foo".try_into().unwrap(),
            odf::dataset::make_seed_block(
                did_generator.generate_dataset_id().0,
                odf::DatasetKind::Root,
                time_source.now(),
            ),
        )
        .await
        .unwrap();

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let res = query_svc.tail(&"foo".try_into().unwrap(), 0, 10).await;
    assert_matches!(res, Err(QueryError::DatasetSchemaNotAvailable(_)));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        Err(QueryError::DataFusionError(DataFusionError {
            source: datafusion::common::DataFusionError::Plan(s),
            ..
        }))  if s.contains("table 'kamu.kamu.foo' not found")
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_sql_statement_not_found() {
    let mut mock_authorizer = MockDatasetActionAuthorizer::new();
    mock_authorizer
        .expect_check_action_allowed()
        .returning(|_, _| Ok(()));
    mock_authorizer
        .expect_filter_datasets_allowing()
        .returning(|_, _| Ok(vec![]));

    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(tempdir.path(), mock_authorizer);

    let _ = create_test_dataset(&catalog, tempdir.path()).await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) from does_not_exist";
    let result = query_svc
        .sql_statement(statement, QueryOptions::default())
        .await;

    assert_matches!(
        result,
        Err(QueryError::DataFusionError(DataFusionError {
            source: ::datafusion::common::DataFusionError::Plan(s),
            ..
        }))  if s.contains("table 'kamu.kamu.does_not_exist' not found")
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

    let dataset = create_test_dataset(&catalog, tempdir.path()).await;

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let statement = "select count(*) as num_records from foobar";
    let result = query_svc
        .sql_statement(
            statement,
            QueryOptions {
                input_datasets: BTreeMap::from([(
                    dataset.dataset_handle.id,
                    QueryOptionsDataset {
                        alias: "foobar".to_string(),
                        ..Default::default()
                    },
                )]),
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
                input_datasets: BTreeMap::from([(
                    odf::DatasetID::new_seeded_ed25519(b"does-not-exist"),
                    QueryOptionsDataset {
                        alias: dataset_alias.to_string(),
                        ..Default::default()
                    },
                )]),
            },
        )
        .await;

    assert_matches!(
        result,
        Err(QueryError::DatasetNotFound(odf::dataset::DatasetNotFoundError {
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

    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();
    let ctx = SessionContext::new();

    // Dataset init
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_create = dataset_storage_unit_writer
        .create_dataset(
            &foo_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(odf::DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap();
    let foo_id = &foo_create.dataset_handle.id;

    let mut writer = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        ResolvedDataset::from(&foo_create),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
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
                block_hash: foo_create
                    .dataset
                    .as_metadata_chain()
                    .resolve_ref(&odf::BlockRef::Head)
                    .await
                    .unwrap()
            }
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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
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
                input_datasets: res
                    .state
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

    let dataset_storage_unit_writer = catalog
        .get_one::<dyn odf::DatasetStorageUnitWriter>()
        .unwrap();
    let ctx = SessionContext::new();

    // Dataset `foo`
    let foo_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("foo"));
    let foo_created = dataset_storage_unit_writer
        .create_dataset(
            &foo_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(odf::DatasetKind::Root)
                    .id_random()
                    .build(),
            )
            .build_typed(),
        )
        .await
        .unwrap();
    let foo_id = &foo_created.dataset_handle.id;

    let mut writer_foo = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        ResolvedDataset::from(&foo_created),
        &odf::BlockRef::Head,
        None,
    )
    .await
    .unwrap();

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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
            },
        )
        .await
        .unwrap();

    // Dataset `bar`
    let bar_alias = odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("bar"));
    let bar_created = dataset_storage_unit_writer
        .create_dataset(
            &bar_alias,
            MetadataFactory::metadata_block(
                MetadataFactory::seed(odf::DatasetKind::Root)
                    .id_random()
                    .build(),
            )
            .build_typed(),
        )
        .await
        .unwrap();
    let bar_id = &bar_created.dataset_handle.id;

    let mut writer_bar = DataWriterDataFusion::from_metadata_chain(
        ctx.clone(),
        ResolvedDataset::from(&bar_created),
        &odf::BlockRef::Head,
        None,
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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
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
                    block_hash: foo_created
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
                    block_hash: bar_created
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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
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
                data_staging_path: tempdir.path().join(".temp-data.parquet"),
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
                input_datasets: res
                    .state
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
                input_datasets: BTreeMap::from([
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
                ]),
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
