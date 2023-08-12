// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use kamu::domain::*;
use kamu::testing::{LocalS3Server, MetadataFactory, ParquetWriterHelper};
use kamu::utils::s3_context::S3Context;
use kamu::*;
use kamu_data_utils::data::format::JsonArrayWriter;
use opendatafabric::*;
use tempfile::TempDir;

use crate::utils::mock_dataset_action_authorizer;
use crate::MockDatasetActionAuthorizer;

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_test_dataset(catalog: &dill::Catalog, tempdir: &Path) -> DatasetAlias {
    let dataset_repo = catalog.get_one::<dyn DatasetRepository>().unwrap();
    let dataset_alias = DatasetAlias::new(None, DatasetName::new_unchecked("foo"));

    let dataset = dataset_repo
        .create_dataset(
            &dataset_alias,
            MetadataFactory::metadata_block(MetadataFactory::seed(DatasetKind::Root).build())
                .build_typed(),
        )
        .await
        .unwrap()
        .dataset;

    // Write data spread over two commits
    let tmp_data_path = tempdir.join("data");
    let schema = Arc::new(Schema::new(vec![
        Field::new("offset", DataType::UInt64, false),
        Field::new("blah", DataType::Utf8, false),
    ]));
    let batches = [
        (
            UInt64Array::from(vec![0, 1]),
            StringArray::from(vec!["a", "b"]),
        ),
        (UInt64Array::from(vec![2]), StringArray::from(vec!["c"])),
    ];

    let mut offset = 0;
    for (a, b) in batches {
        let record_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(a), Arc::new(b)]).unwrap();
        ParquetWriterHelper::from_record_batch(&tmp_data_path, &record_batch).unwrap();

        dataset
            .commit_add_data(
                AddDataParams {
                    input_checkpoint: None,
                    output_data: Some(OffsetInterval {
                        start: offset,
                        end: offset + record_batch.num_rows() as i64 - 1,
                    }),
                    output_watermark: None,
                    source_state: None,
                },
                Some(OwnedFile::new(tmp_data_path.clone())),
                None,
                CommitOpts::default(),
            )
            .await
            .unwrap();

        offset += record_batch.num_rows() as i64;
    }

    dataset_alias
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn create_catalog_with_local_workspace(
    tempdir: &Path,
    dataset_action_authorizer: MockDatasetActionAuthorizer,
) -> dill::Catalog {
    dill::CatalogBuilder::new()
        .add_builder(
            dill::builder_for::<DatasetRepositoryLocalFs>()
                .with_root(tempdir.join("datasets"))
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryLocalFs>()
        .add::<QueryServiceImpl>()
        .bind::<dyn QueryService, QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>()
        .add_value(ObjectStoreBuilderLocalFs::new())
        .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderLocalFs>()
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
        .add_builder(
            dill::builder_for::<DatasetRepositoryS3>()
                .with_s3_context(s3_context.clone())
                .with_multi_tenant(false),
        )
        .bind::<dyn DatasetRepository, DatasetRepositoryS3>()
        .add::<QueryServiceImpl>()
        .bind::<dyn QueryService, QueryServiceImpl>()
        .add::<ObjectStoreRegistryImpl>()
        .bind::<dyn ObjectStoreRegistry, ObjectStoreRegistryImpl>()
        .add_value(ObjectStoreBuilderLocalFs::new())
        .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderLocalFs>()
        .add_value(ObjectStoreBuilderS3::new(s3_context, true))
        .bind::<dyn ObjectStoreBuilder, ObjectStoreBuilderS3>()
        .add_value(CurrentAccountSubject::new_test())
        .add_value(dataset_action_authorizer)
        .bind::<dyn auth::DatasetActionAuthorizer, MockDatasetActionAuthorizer>()
        .build()
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let schema = query_svc.get_schema(&dataset_ref).await.unwrap();
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

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_schema_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        mock_dataset_action_authorizer::expecting_read_mock(1),
    )
    .await;
    test_dataset_schema_common(catalog, &tempdir).await;
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        mock_dataset_action_authorizer::expecting_read_mock(1),
    )
    .await;
    test_dataset_schema_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_schema_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let result = query_svc.get_schema(&dataset_ref).await;
    assert_matches!(result, Err(QueryError::Access(_)));
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_schema_unauthorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        mock_dataset_action_authorizer::denying_mock(TEST_ACCOUNT_NAME),
    )
    .await;
    test_dataset_schema_unauthorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_dataset_schema_unauthorized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        mock_dataset_action_authorizer::denying_mock(TEST_ACCOUNT_NAME),
    )
    .await;
    test_dataset_schema_unauthorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let df = query_svc.tail(&dataset_ref, 1, 1).await.unwrap();
    let record_batches = df.collect().await.unwrap();

    let mut buf = Vec::new();
    let mut writer = Box::new(JsonArrayWriter::new(&mut buf));
    record_batches.iter().for_each(|b| writer.write(b).unwrap());
    writer.finish().unwrap();

    let data_content = String::from_utf8(buf).unwrap();
    let data_json = serde_json::from_str::<serde_json::Value>(data_content.as_str()).unwrap();

    assert_eq!(data_json, serde_json::json!([{"blah": "b", "offset": 1}]));
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_tail_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        mock_dataset_action_authorizer::expecting_read_mock(1),
    )
    .await;
    test_dataset_tail_common(catalog, &tempdir).await;
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        mock_dataset_action_authorizer::expecting_read_mock(1),
    )
    .await;
    test_dataset_tail_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////

async fn test_dataset_tail_unauthorized_common(catalog: dill::Catalog, tempdir: &TempDir) {
    let dataset_alias = create_test_dataset(&catalog, tempdir.path()).await;
    let dataset_ref = DatasetRef::from(dataset_alias);

    let query_svc = catalog.get_one::<dyn QueryService>().unwrap();
    let result = query_svc.tail(&dataset_ref, 1, 1).await;
    assert_matches!(result, Err(QueryError::Access(_)));
}

#[test_log::test(tokio::test)]
#[cfg_attr(not(unix), ignore)] // TODO: DataFusion crashes on windows
async fn test_dataset_tail_unauhtorized_local_fs() {
    let tempdir = tempfile::tempdir().unwrap();
    let catalog = create_catalog_with_local_workspace(
        tempdir.path(),
        mock_dataset_action_authorizer::denying_mock(TEST_ACCOUNT_NAME),
    )
    .await;
    test_dataset_tail_unauthorized_common(catalog, &tempdir).await;
}

#[test_group::group(containerized)]
#[test_log::test(tokio::test)]
async fn test_dataset_tail_unauthroized_s3() {
    let s3 = LocalS3Server::new().await;
    let catalog = create_catalog_with_s3_workspace(
        &s3,
        mock_dataset_action_authorizer::denying_mock(TEST_ACCOUNT_NAME),
    )
    .await;
    test_dataset_tail_unauthorized_common(catalog, &s3.tmp_dir).await;
}

/////////////////////////////////////////////////////////////////////////////////////////
