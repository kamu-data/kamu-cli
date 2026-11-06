// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use odf::metadata::serde::yaml::{YamlDatasetSnapshotSerializer, YamlMetadataBlockDeserializer};
use odf::metadata::serde::{DatasetSnapshotSerializer, MetadataBlockDeserializer};
use regex::Regex;
use serde::Deserialize;
use url::Url;

use crate::{ExecuteCommandResult, KamuCliPuppet};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuCliPuppetExt {
    async fn assert_success_command_execution<CmdIt, CmdItem>(
        &self,
        cmd: CmdIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send;

    async fn assert_success_command_execution_with_input<CmdIt, CmdItem, InputIt>(
        &self,
        cmd: CmdIt,
        input: InputIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        InputIt: Into<Vec<u8>> + Send;

    async fn assert_success_command_execution_with_env<CmdIt, CmdItem, EnvItem>(
        &self,
        cmd: CmdIt,
        env_vars: Vec<(EnvItem, EnvItem)>,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        EnvItem: AsRef<std::ffi::OsStr> + Send;

    async fn assert_failure_command_execution<CmdIt, CmdItem>(
        &self,
        cmd: CmdIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send;

    async fn assert_failure_command_execution_with_input<CmdIt, CmdItem, InputIt>(
        &self,
        cmd: CmdIt,
        input: InputIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        InputIt: Into<Vec<u8>> + Send;

    async fn list_datasets(&self) -> Vec<DatasetRecord>;

    async fn list_datasets_ex(&self, options: DatasetListOptions) -> Vec<DatasetRecord>;

    async fn add_dataset(&self, dataset_snapshot: odf::DatasetSnapshot, options: AddDatasetOptions);

    async fn list_blocks(&self, dataset_name: &odf::DatasetName) -> Vec<BlockRecord>;

    async fn log_stdout(&self, dataset_name: &odf::DatasetName, args: &[&str]) -> String;

    /// `HEAD` of every dataset as written to the workspace storage, which can
    /// lag the database until the Outbox is processed
    fn list_storage_heads(&self) -> Vec<odf::Multihash>;

    async fn ingest_data(&self, dataset_name: &odf::DatasetName, data: &str);

    async fn get_list_of_repo_aliases(&self, dataset_ref: &odf::DatasetRef) -> Vec<RepoAlias>;

    async fn get_list_of_repos(&self) -> Vec<RepoRecord>;

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send;

    async fn start_api_server(self, e2e_data_file_path: PathBuf) -> ServerOutput;

    async fn assert_player_scores_dataset_data(&self, expected_player_scores_table: &str);

    async fn assert_last_data_slice(
        &self,
        dataset_alias: &odf::DatasetAlias,
        expected_schema: &str,
        expected_data: &str,
    );

    async fn login_to_node(&self, node_url: &Url, account_name: &str, password: &str);

    async fn gql_query(&self, query: &str) -> String;

    async fn create_account(&self, account_name: &odf::AccountName);

    async fn delete_account(&self, account_name: &odf::AccountName) -> DeleteAccountResult;

    /// Creates the accounts in a multi-tenant workspace, in the given order
    async fn create_accounts<const N: usize>(
        &self,
        account_names: [&str; N],
    ) -> [odf::AccountName; N];

    /// The record of a dataset of the current account, or of any account with
    /// `options` listing all accounts
    async fn get_dataset_record(
        &self,
        dataset_name: &odf::DatasetName,
        options: DatasetListOptions,
    ) -> DatasetRecord;

    async fn get_dataset_id(&self, dataset_name: &odf::DatasetName) -> odf::DatasetID;

    /// `HEAD` of every dataset by its owner. Lists them as the default
    /// account, which it leaves set
    async fn list_heads_by_owner(&mut self) -> HashMap<odf::AccountName, odf::Multihash>;

    async fn list_block_hashes(&self, dataset_name: &odf::DatasetName) -> Vec<odf::Multihash>;

    /// Commits a metadata event through the workspace's GraphQL API
    async fn commit_event(&self, dataset_name: &odf::DatasetName, event_yaml: &str);

    /// Makes a dataset of the current account private through the workspace's
    /// GraphQL API
    async fn set_dataset_private(&self, dataset_name: &odf::DatasetName);

    /// Runs a command that must fail and checks that its stderr contains each
    /// of `expected_stderr` in that order, naming `case` in any assertion
    /// failure
    async fn assert_failure_stderr_in_order(
        &self,
        case: &str,
        cmd: &[&str],
        stdin: Option<&str>,
        expected_stderr: &[&str],
    ) -> OrderedStderr;

    /// The directory `dir_name` of the workspace as a URL
    fn workspace_dir_url(&self, dir_name: &str) -> Url;

    /// A dataset URL in the directory `dir_name` of the workspace
    fn workspace_dataset_url(&self, dir_name: &str, dataset_name: &str) -> Url;

    /// Where the workspace keeps the alias config of a dataset
    fn dataset_alias_config_path(&self, dataset_id: &odf::DatasetID) -> PathBuf;

    /// The entries of the workspace's run directory
    fn list_run_dir(&self) -> Vec<PathBuf>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuCliPuppetExt for KamuCliPuppet {
    async fn login_to_node(&self, node_url: &Url, account_name: &str, password: &str) {
        let url = node_url.as_str();

        self.assert_success_command_execution(
            ["login", "password", account_name, password, url],
            None,
            Some([format!("Login successful: {url}").as_str()]),
        )
        .await;
    }

    async fn list_datasets(&self) -> Vec<DatasetRecord> {
        self.list_datasets_ex(Default::default()).await
    }

    async fn list_datasets_ex(&self, options: DatasetListOptions) -> Vec<DatasetRecord> {
        let mut cmd = vec!["list", "--wide", "--output-format", "json"];
        if options.all_accounts {
            cmd.push("--all-accounts");
        }

        let assert = self.execute(cmd).await.success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        let mut res: Vec<DatasetRecord> = serde_json::from_str(stdout).unwrap();
        res.sort_by(|a, b| a.name.cmp(&b.name));
        res
    }

    async fn add_dataset(
        &self,
        dataset_snapshot: odf::DatasetSnapshot,
        options: AddDatasetOptions,
    ) {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();

        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.as_file().write_all(&content).unwrap();
        f.flush().unwrap();

        let mut cmd = vec!["add", f.path().to_str().unwrap()];
        let maybe_visibility = options.visibility.map(|v| format!("{v}"));

        if let Some(visibility) = &maybe_visibility {
            cmd.extend(["--visibility", visibility.as_str()]);
        }

        self.execute(cmd).await.success();
    }

    async fn get_list_of_repo_aliases(&self, dataset_ref: &odf::DatasetRef) -> Vec<RepoAlias> {
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

    async fn get_list_of_repos(&self) -> Vec<RepoRecord> {
        let assert = self
            .execute(["repo", "list", "--output-format", "json"])
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

    async fn list_blocks(&self, dataset_name: &odf::DatasetName) -> Vec<BlockRecord> {
        self.log_stdout(dataset_name, &["--output-format", "ndjson"])
            .await
            .lines()
            .map(|line| {
                let mut record: serde_json::Map<String, serde_json::Value> =
                    serde_json::from_str(line).unwrap();

                let block_hash = record.remove("blockHash").unwrap();
                let block_hash =
                    odf::Multihash::from_multibase(block_hash.as_str().unwrap()).unwrap();

                // What remains is the YAML manifest, and JSON is valid YAML
                let manifest = serde_json::to_vec(&record).unwrap();
                let block = YamlMetadataBlockDeserializer {}
                    .read_manifest(&manifest)
                    .unwrap();

                BlockRecord { block_hash, block }
            })
            .collect()
    }

    fn list_storage_heads(&self) -> Vec<odf::Multihash> {
        let datasets_dir = self.workspace_path().join(".kamu").join("datasets");

        std::fs::read_dir(datasets_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path().join("refs").join("head"))
            .filter(|head_path| head_path.is_file())
            .map(|head_path| {
                let head = std::fs::read_to_string(head_path).unwrap();
                odf::Multihash::from_multibase(head.trim()).unwrap()
            })
            .collect()
    }

    async fn log_stdout(&self, dataset_name: &odf::DatasetName, args: &[&str]) -> String {
        let assert = self
            .execute(["log"].iter().chain(args).chain(&[dataset_name.as_str()]))
            .await
            .success();

        String::from_utf8(assert.get_output().stdout.clone()).unwrap()
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
                // Without unstable "offset" column
                indoc::indoc!(
                    r#"
                    SELECT op,
                           system_time,
                           match_time,
                           match_id,
                           player_id,
                           score
                    FROM 'player-scores'
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
        dataset_alias: &odf::DatasetAlias,
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
                &dataset_alias.to_string(),
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
                    file_decryption_properties: None,
                    metadata_size_hint: None,
                },
            )
            .await
            .unwrap();

        odf::utils::testing::assert_data_eq(df.clone().into(), expected_data).await;
        odf::utils::testing::assert_schema_eq(df.schema(), expected_schema);
    }

    async fn ingest_data(&self, dataset_name: &odf::DatasetName, data: &str) {
        self.execute_with_input(["ingest", dataset_name, "--stdin"], data)
            .await
            .success();
    }

    async fn assert_success_command_execution<CmdIt, CmdItem>(
        &self,
        cmd: CmdIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
    {
        assert_execute_command_result(
            &self.execute(cmd).await.success(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_success_command_execution_with_input<CmdIt, CmdItem, InputIt>(
        &self,
        cmd: CmdIt,
        input: InputIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        InputIt: Into<Vec<u8>> + Send,
    {
        assert_execute_command_result(
            &self.execute_with_input(cmd, input).await.success(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_success_command_execution_with_env<CmdIt, CmdItem, EnvItem>(
        &self,
        cmd: CmdIt,
        env_vars: Vec<(EnvItem, EnvItem)>,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        EnvItem: AsRef<std::ffi::OsStr> + Send,
    {
        assert_execute_command_result(
            &self.execute_with_env(cmd, env_vars).await.success(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_failure_command_execution<CmdIt, CmdItem>(
        &self,
        cmd: CmdIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
    {
        assert_execute_command_result(
            &self.execute(cmd).await.failure(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn assert_failure_command_execution_with_input<CmdIt, CmdItem, InputIt>(
        &self,
        cmd: CmdIt,
        input: InputIt,
        maybe_expected_stdout: Option<&str>,
        maybe_expected_stderr: Option<impl IntoIterator<Item = &str> + Send>,
    ) where
        CmdIt: IntoIterator<Item = CmdItem> + Send,
        CmdItem: AsRef<std::ffi::OsStr> + Send,
        InputIt: Into<Vec<u8>> + Send,
    {
        assert_execute_command_result(
            &self.execute_with_input(cmd, input).await.failure(),
            maybe_expected_stdout,
            maybe_expected_stderr,
        );
    }

    async fn gql_query(&self, query: &str) -> String {
        let assert = self
            .execute(["system", "api-server", "gql-query", query])
            .await
            .success();

        std::str::from_utf8(&assert.get_output().stdout)
            .unwrap()
            .to_string()
    }

    async fn create_account(&self, account_name: &odf::AccountName) {
        let res = self
            .gql_query(
                &indoc::indoc!(
                    r#"
                    mutation {
                      accounts {
                        createAccount(accountName: "$accountName") {
                          message
                        }
                      }
                    }
                    "#
                )
                .replace("$accountName", account_name.as_str()),
            )
            .await;

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                {
                  "accounts": {
                    "createAccount": {
                      "message": "Account created"
                    }
                  }
                }
                "#
            ),
            res
        );
    }

    async fn delete_account(&self, account_name: &odf::AccountName) -> DeleteAccountResult {
        let res = self
            .gql_query(
                &indoc::indoc!(
                    r#"
                    mutation {
                      accounts {
                        byName(accountName: "$accountName") {
                          delete {
                            message
                          }
                        }
                      }
                    }
                    "#
                )
                .replace("$accountName", account_name.as_str()),
            )
            .await;

        const DELETED: &str = indoc::indoc!(
            r#"
            {
              "accounts": {
                "byName": {
                  "delete": {
                    "message": "Account deleted"
                  }
                }
              }
            }
            "#
        );
        const NOT_FOUND: &str = indoc::indoc!(
            r#"
            {
              "accounts": {
                "byName": null
              }
            }
            "#
        );

        if res == DELETED {
            DeleteAccountResult::Deleted
        } else if res == NOT_FOUND {
            DeleteAccountResult::NotFound
        } else {
            DeleteAccountResult::UnexpectedResult(res)
        }
    }

    async fn create_accounts<const N: usize>(
        &self,
        account_names: [&str; N],
    ) -> [odf::AccountName; N] {
        let account_names = account_names.map(odf::AccountName::new_unchecked);
        for account_name in &account_names {
            self.create_account(account_name).await;
        }
        account_names
    }

    async fn get_dataset_record(
        &self,
        dataset_name: &odf::DatasetName,
        options: DatasetListOptions,
    ) -> DatasetRecord {
        self.list_datasets_ex(options)
            .await
            .into_iter()
            .find(|dataset| &dataset.name == dataset_name)
            .unwrap()
    }

    async fn get_dataset_id(&self, dataset_name: &odf::DatasetName) -> odf::DatasetID {
        self.get_dataset_record(dataset_name, DatasetListOptions::default())
            .await
            .id
    }

    async fn list_heads_by_owner(&mut self) -> HashMap<odf::AccountName, odf::Multihash> {
        self.set_account(None);
        self.list_datasets_ex(DatasetListOptions::builder().all_accounts(true).build())
            .await
            .into_iter()
            .map(|d| (d.owner.unwrap(), d.head))
            .collect()
    }

    async fn list_block_hashes(&self, dataset_name: &odf::DatasetName) -> Vec<odf::Multihash> {
        self.list_blocks(dataset_name)
            .await
            .into_iter()
            .map(|b| b.block_hash)
            .collect()
    }

    async fn commit_event(&self, dataset_name: &odf::DatasetName, event_yaml: &str) {
        let dataset_id = self.get_dataset_id(dataset_name).await;
        let response = self
            .gql_query(&format!(
                r#"mutation {{ datasets {{ byId(datasetId: "{}") {{ metadata {{ chain {{ commitEvent(event: "{}", eventFormat: YAML) {{ __typename }} }} }} }} }} }}"#,
                dataset_id.as_did_str(),
                event_yaml.escape_default(),
            ))
            .await;

        assert!(response.contains("CommitResultSuccess"), "{response}");
    }

    async fn set_dataset_private(&self, dataset_name: &odf::DatasetName) {
        let dataset_id = self.get_dataset_id(dataset_name).await;
        let response = self
            .gql_query(&format!(
                r#"mutation {{ datasets {{ byId(datasetId: "{}") {{ setVisibility(visibility: {{ private: {{}} }}) {{ __typename }} }} }} }}"#,
                dataset_id.as_did_str(),
            ))
            .await;

        assert!(
            response.contains("SetDatasetVisibilityResultSuccess"),
            "{response}"
        );
    }

    async fn assert_failure_stderr_in_order(
        &self,
        case: &str,
        cmd: &[&str],
        stdin: Option<&str>,
        expected_stderr: &[&str],
    ) -> OrderedStderr {
        let result = match stdin {
            Some(input) => self.execute_with_input(cmd, input).await,
            None => self.execute(cmd).await,
        };
        let output = result.get_output();
        let stderr = String::from_utf8_lossy(&output.stderr).into_owned();

        assert!(
            !output.status.success(),
            "Case '{case}': the command succeeded\n{stderr}"
        );
        let mut end_of_matches = 0;
        for expected in expected_stderr {
            let Some(found_at) = stderr[end_of_matches..].find(expected) else {
                panic!(
                    "Case '{case}': stderr has no '{expected}' after the earlier matches\n{stderr}"
                );
            };
            end_of_matches += found_at + expected.len();
        }

        OrderedStderr {
            stderr,
            end_of_matches,
        }
    }

    fn workspace_dir_url(&self, dir_name: &str) -> Url {
        Url::from_directory_path(self.workspace_path().join(dir_name)).unwrap()
    }

    fn workspace_dataset_url(&self, dir_name: &str, dataset_name: &str) -> Url {
        self.workspace_dir_url(dir_name).join(dataset_name).unwrap()
    }

    fn dataset_alias_config_path(&self, dataset_id: &odf::DatasetID) -> PathBuf {
        self.workspace_path()
            .join(".kamu")
            .join("datasets")
            .join(dataset_id.as_multibase().to_stack_string().as_str())
            .join("info")
            .join("config")
    }

    fn list_run_dir(&self) -> Vec<PathBuf> {
        std::fs::read_dir(self.workspace_path().join(".kamu").join("run"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The stderr of a failed command in which the expected parts were found in
/// order
#[derive(Debug)]
pub struct OrderedStderr {
    pub stderr: String,
    end_of_matches: usize,
}

impl OrderedStderr {
    /// The stderr after the last expected part
    pub fn after_matches(&self) -> &str {
        &self.stderr[self.end_of_matches..]
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum DeleteAccountResult {
    Deleted,
    NotFound,
    UnexpectedResult(String),
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
    pub id: odf::DatasetID,
    pub owner: Option<odf::AccountName>,
    pub name: odf::DatasetName,
    // CLI returns regular ENUM DatasetKind(Root/Derivative) for local datasets
    // but for remote it is Remote(DatasetKind) type
    pub kind: String,
    pub head: odf::Multihash,
    pub pulled: Option<DateTime<Utc>>,
    pub records: usize,
    pub blocks: usize,
    pub size: usize,
    pub watermark: Option<DateTime<Utc>>,
    pub visibility: Option<odf::DatasetVisibility>,
}

#[derive(Debug, bon::Builder, Default)]
pub struct DatasetListOptions {
    all_accounts: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct RepoAlias {
    pub dataset: odf::DatasetAlias,
    pub kind: String,
    pub alias: String,
}

#[derive(Debug, PartialEq, Eq)]
pub struct BlockRecord {
    pub block_hash: odf::Multihash,
    pub block: odf::MetadataBlock,
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "PascalCase", deny_unknown_fields)]
pub struct RepoRecord {
    pub name: odf::RepoName,
    pub url: Url,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, bon::Builder, Default)]
pub struct AddDatasetOptions {
    visibility: Option<odf::DatasetVisibility>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
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

        let mut stderr_search_index = 0;
        for expected_stderr_item_re in expected_stderr_items {
            let re = Regex::new(expected_stderr_item_re).unwrap();

            if let Some(mat) = re.find(&stderr[stderr_search_index..]) {
                stderr_search_index += mat.end();
            } else {
                assert!(
                    re.is_match(stderr),
                    "Expected found regex match:\n'{re}'\nUnexpected output:\n{stderr}",
                );

                panic!("Expected found regex match:\n'{re}'\nUnexpected output:\n{stderr}");
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
