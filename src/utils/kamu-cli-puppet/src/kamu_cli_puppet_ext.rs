// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use opendatafabric::serde::yaml::{DatasetKindDef, YamlDatasetSnapshotSerializer};
use opendatafabric::serde::DatasetSnapshotSerializer;
use opendatafabric::{DatasetID, DatasetKind, DatasetName, DatasetRef, DatasetSnapshot, Multihash};
use serde::Deserialize;

use crate::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuCliPuppetExt {
    async fn list_datasets(&self) -> Vec<DatasetRecord>;

    async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot);

    async fn get_list_of_repo_aliases(&self, dataset_ref: &DatasetRef) -> Vec<RepoAlias>;

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send;

    async fn start_api_server(self, e2e_data_file_path: PathBuf) -> ServerOutput;

    async fn assert_last_data_slice(
        &self,
        dataset_name: &DatasetName,
        expected_schema: &str,
        expected_data: &str,
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuCliPuppetExt for KamuCliPuppet {
    async fn list_datasets(&self) -> Vec<DatasetRecord> {
        let assert = self
            .execute(["list", "--wide", "--output-format", "json"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        serde_json::from_str(stdout).unwrap()
    }

    async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();

        let mut f = tempfile::NamedTempFile::new().unwrap();

        f.as_file().write_all(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()])
            .await
            .success();
    }

    async fn get_list_of_repo_aliases(&self, dataset_ref: &DatasetRef) -> Vec<RepoAlias> {
        let assert = self
            .execute([
                "repo",
                "alias",
                "list",
                dataset_ref.to_string().as_str(),
                "--output-format",
                "json",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        serde_json::from_str(stdout).unwrap()
    }

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send,
    {
        let assert = self
            .execute([
                "complete",
                input.into().as_str(),
                current.to_string().as_str(),
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        stdout.lines().map(ToString::to_string).collect()
    }

    async fn start_api_server(self, e2e_data_file_path: PathBuf) -> ServerOutput {
        let host = Ipv4Addr::LOCALHOST.to_string();

        let assert = self
            .execute([
                "--e2e-output-data-path",
                e2e_data_file_path.to_str().unwrap(),
                "system",
                "api-server",
                "--address",
                host.as_str(),
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout)
            .unwrap()
            .to_owned();
        let stderr = std::str::from_utf8(&assert.get_output().stderr)
            .unwrap()
            .to_owned();

        ServerOutput { stdout, stderr }
    }

    async fn assert_last_data_slice(
        &self,
        dataset_name: &DatasetName,
        expected_schema: &str,
        expected_data: &str,
    ) {
        let assert = self
            .execute([
                "system",
                "e2e",
                "--action",
                "get-last-data-block-path",
                "--dataset",
                dataset_name.as_str(),
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        let part_file: PathBuf = stdout.trim().parse().unwrap();

        let ctx = SessionContext::new();
        let df = ctx
            .read_parquet(
                vec![part_file.to_string_lossy().to_string()],
                ParquetReadOptions {
                    file_extension: "",
                    table_partition_cols: Vec::new(),
                    parquet_pruning: None,
                    skip_metadata: None,
                    schema: None,
                    file_sort_order: Vec::new(),
                },
            )
            .await
            .unwrap();

        kamu_data_utils::testing::assert_data_eq(df.clone(), expected_data).await;
        kamu_data_utils::testing::assert_schema_eq(df.schema(), expected_schema);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ServerOutput {
    pub stdout: String,
    pub stderr: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct DatasetRecord {
    #[serde(rename = "ID")]
    pub id: DatasetID,
    pub name: DatasetName,
    #[serde(with = "DatasetKindDef")]
    pub kind: DatasetKind,
    pub head: Multihash,
    pub pulled: Option<DateTime<Utc>>,
    pub records: usize,
    pub blocks: usize,
    pub size: usize,
    pub watermark: Option<DateTime<Utc>>,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct RepoAlias {
    pub dataset: DatasetName,
    pub kind: String,
    pub alias: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
