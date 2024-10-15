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
use opendatafabric::serde::yaml::{YamlDatasetSnapshotSerializer, YamlMetadataBlockDeserializer};
use opendatafabric::serde::{DatasetSnapshotSerializer, MetadataBlockDeserializer};
use opendatafabric::{
    DatasetID,
    DatasetName,
    DatasetRef,
    DatasetSnapshot,
    MetadataBlock,
    Multihash,
};
use serde::Deserialize;

use crate::{ExecuteCommandResult, KamuCliPuppet};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuCliPuppetExt {
    async fn assert_success_command_execution<I, S>(
        &self,
        cmd: I,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>;

    async fn assert_success_command_execution_with_input<I, S, T>(
        &self,
        cmd: I,
        input: T,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
        T: Into<Vec<u8>> + Send;

    async fn assert_failure_command_execution<I, S>(
        &self,
        cmd: I,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>;

    async fn assert_failure_command_execution_with_input<I, S, T>(
        &self,
        cmd: I,
        input: T,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
        T: Into<Vec<u8>> + Send;

    async fn list_datasets(&self) -> Vec<DatasetRecord>;

    async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot);

    async fn list_blocks(&self, dataset_name: &DatasetName) -> Vec<BlockRecord>;

    async fn ingest_data(&self, dataset_name: &DatasetName, data: &str);

    async fn get_list_of_repo_aliases(&self, dataset_ref: &DatasetRef) -> Vec<RepoAlias>;

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send;

    async fn start_api_server(self, e2e_data_file_path: PathBuf) -> ServerOutput;

    async fn assert_player_scores_dataset_data(&self, expected_player_scores_table: &str);

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

    async fn list_blocks(&self, dataset_name: &DatasetName) -> Vec<BlockRecord> {
        let assert = self
            .execute(["log", dataset_name.as_str(), "--output-format", "yaml"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        // TODO: Don't parse the output, after implementation:
        //       `kamu log`: support `--output-format json`
        //       https://github.com/kamu-data/kamu-cli/issues/887

        stdout
            .split("---")
            .skip(1)
            .map(str::trim)
            .map(|block_data| {
                let Some(pos) = block_data.find('\n') else {
                    unreachable!()
                };
                let (first_line_with_block_hash, metadata_block_str) = block_data.split_at(pos);

                let block_hash = first_line_with_block_hash
                    .strip_prefix("# Block: ")
                    .unwrap();
                let block = YamlMetadataBlockDeserializer {}
                    .read_manifest(metadata_block_str.as_ref())
                    .unwrap();

                BlockRecord {
                    block_hash: Multihash::from_multibase(block_hash).unwrap(),
                    block,
                }
            })
            .collect()
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

    async fn assert_player_scores_dataset_data(&self, expected_player_scores_table: &str) {
        self.assert_success_command_execution(
            [
                "sql",
                "--engine",
                "datafusion",
                "--command",
                // Without unstable "offset" column.
                // For a beautiful output, cut to seconds
                indoc::indoc!(
                    r#"
                    SELECT op,
                           system_time,
                           match_time,
                           match_id,
                           player_id,
                           score
                    FROM "player-scores"
                    ORDER BY match_id, score, player_id;
                    "#
                ),
                "--output-format",
                "table",
            ],
            Some(expected_player_scores_table),
            None::<Vec<&str>>,
        )
        .await;
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

    async fn ingest_data(&self, dataset_name: &DatasetName, data: &str) {
        self.execute_with_input(["ingest", dataset_name, "--stdin"], data)
            .await
            .success();
    }

    async fn assert_success_command_execution<I, S>(
        &self,
        cmd: I,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
    {
        assert_execute_command_result(
            &self.execute(cmd).await.success(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_success_command_execution_with_input<I, S, T>(
        &self,
        cmd: I,
        input: T,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
        T: Into<Vec<u8>> + Send,
    {
        assert_execute_command_result(
            &self.execute_with_input(cmd, input).await.success(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_failure_command_execution<I, S>(
        &self,
        cmd: I,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
    {
        assert_execute_command_result(
            &self.execute(cmd).await.failure(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_failure_command_execution_with_input<I, S, T>(
        &self,
        cmd: I,
        input: T,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        I: IntoIterator<Item = S> + Send,
        S: AsRef<std::ffi::OsStr>,
        T: Into<Vec<u8>> + Send,
    {
        assert_execute_command_result(
            &self.execute_with_input(cmd, input).await.failure(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
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
    // CLI returns regular ENUM DatasetKind(Root/Derivative) for local datasets
    // but for remote it is Remote(DatasetKind) type
    pub kind: String,
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

#[derive(Debug, PartialEq, Eq)]
pub struct BlockRecord {
    pub block_hash: Multihash,
    pub block: MetadataBlock,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn assert_execute_command_result<'a>(
    command_result: &ExecuteCommandResult,
    maybe_expected_stdout: Option<&str>,
    maybe_expected_stderr: Option<impl IntoIterator<Item = &'a str>>,
) {
    let actual_stdout = std::str::from_utf8(&command_result.get_output().stdout).unwrap();

    if let Some(expected_stdout) = maybe_expected_stdout {
        pretty_assertions::assert_eq!(expected_stdout, actual_stdout);
    }

    if let Some(expected_stderr_items) = maybe_expected_stderr {
        let stderr = std::str::from_utf8(&command_result.get_output().stderr).unwrap();

        for expected_stderr_item in expected_stderr_items {
            assert!(
                stderr.contains(expected_stderr_item),
                "Unexpected output:\n{stderr}",
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
