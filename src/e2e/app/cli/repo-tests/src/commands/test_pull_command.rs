// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ffi::OsStr;

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_NAME,
    DATASET_DERIVATIVE_LEADERBOARD_QUERY,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    city_population_root_snapshot,
    leaderboard_set_transform_event,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use url::Url;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::utils::{
    add_player_scores_with_derivative,
    assert_case_fails,
    commit_event,
    create_alice_and_bob,
    dataset_id,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_SNAPSHOT_STR: &str = indoc::indoc!(
    r#"
    kind: DatasetSnapshot
    version: 1
    content:
      name: test.pull-from-file
      kind: Root
      metadata:
        - kind: SetPollingSource
          fetch:
            kind: Url
            url: file://${{ env.data_dir || env.workspace_dir }}/${{ env.data_file || 'data.csv' }}
          read:
            kind: Csv
            header: true
            separator: ','
          merge:
            kind: snapshot
            primaryKey:
              - city
    "#
);

const DATASET_INGEST_DATA: &str = indoc::indoc!(
    r#"
    city,population
    A,1000
    B,2000
    C,3000
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_env_var_template_default_value
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value_st(kamu: KamuCliPuppet) {
    test_pull_env_var_template_default_value(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value_mt(kamu: KamuCliPuppet) {
    test_pull_env_var_template_default_value(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_env_var_template_default_value_missing_values
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value_missing_values_st(kamu: KamuCliPuppet) {
    test_pull_env_var_template_default_value_missing_values(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value_missing_values_mt(kamu: KamuCliPuppet) {
    test_pull_env_var_template_default_value_missing_values(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_env_var_from_labelled_variable_set
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_from_labelled_variable_set_st(kamu: KamuCliPuppet) {
    test_pull_env_var_from_labelled_variable_set(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_set_watermark
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_set_watermark_st(kamu: KamuCliPuppet) {
    test_pull_set_watermark(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_set_watermark_mt(kamu: KamuCliPuppet) {
    test_pull_set_watermark(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_reset_derivative
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reset_derivative_st(kamu: KamuCliPuppet) {
    test_pull_reset_derivative(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reset_derivative_mt(kamu: KamuCliPuppet) {
    test_pull_reset_derivative(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_st(kamu: KamuCliPuppet) {
    test_pull_derivative(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_mt(kamu: KamuCliPuppet) {
    test_pull_derivative(kamu).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_reports_why_it_cannot_run
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_reports_why_it_cannot_run(kamu: KamuCliPuppet) {
    let root_name = DATASET_ROOT_PLAYER_NAME.as_str();
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();
    kamu.execute_with_input(
        ["add", "--stdin"],
        indoc::indoc!(
            r#"
            kind: DatasetSnapshot
            version: 1
            content:
              name: no-transform
              kind: Derivative
              metadata: []
            "#
        ),
    )
    .await
    .success();

    let derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str();
    assert_case_fails(
        &kamu,
        "never pulled root",
        &["pull", derivative_name],
        None,
        &[
            "Failed to update 1 dataset(s)",
            &format!(
                "Failed to pull {derivative_name}: Input dataset {root_name} has no data yet, \
                 pull it first"
            ),
        ],
    )
    .await;
    assert_case_fails(
        &kamu,
        "without transform, with other datasets",
        &["pull", "no-transform", root_name],
        None,
        &["Failed to pull no-transform: Dataset does not define a transform"],
    )
    .await;

    // The engine is only looked up once there is input data to transform
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "unknown-engine"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .replace("engine: datafusion", "engine: no-such-engine"),
    )
    .await
    .success();
    assert_case_fails(
        &kamu,
        "unknown engine",
        &["pull", "unknown-engine"],
        None,
        &["Failed to pull unknown-engine: Unsupported engine no-such-engine"],
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_failed_alias_save_creates_no_dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_failed_alias_save_creates_no_dataset(kamu: KamuCliPuppet) {
    let source = KamuCliPuppet::new_workspace_tmp_single_tenant().await;
    source
        .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    let dataset_name = DATASET_ROOT_PLAYER_NAME.as_str();
    source
        .execute_with_input(
            [
                "ingest",
                dataset_name,
                "--stdin",
                "--source-name",
                "default",
            ],
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        )
        .await
        .success();

    let dataset_url = Url::from_directory_path(kamu.workspace_path().join("repo"))
        .unwrap()
        .join(dataset_name)
        .unwrap();
    source
        .execute(["push", dataset_name, "--to", dataset_url.as_str()])
        .await
        .success();

    // A directory where the dataset's alias config belongs makes saving the
    // pull alias fail after the dataset is created in the same transaction
    let dataset_id = &source.list_datasets().await[0].id;
    let alias_config_path = kamu
        .workspace_path()
        .join(".kamu")
        .join("datasets")
        .join(dataset_id.as_multibase().to_stack_string().as_str())
        .join("info")
        .join("config");
    std::fs::create_dir_all(&alias_config_path).unwrap();

    let sync_failure = format!(
        "Failed to sync {dataset_name} from {}",
        regex::escape(dataset_url.as_str())
    );
    kamu.assert_failure_command_execution(
        ["pull", dataset_url.as_str()],
        None,
        Some([r#"Failed to update 1 dataset\(s\)"#, sync_failure.as_str()]),
    )
    .await;
    std::fs::remove_dir_all(&alias_config_path).unwrap();
    assert_eq!(kamu.list_datasets().await.len(), 0);

    // The next pull continues into the transferred blocks and saves the alias
    kamu.execute(["pull", dataset_url.as_str()]).await.success();

    let datasets = kamu.list_datasets().await;
    assert_eq!(datasets.len(), 1);
    assert_eq!(datasets[0].name.as_str(), dataset_name);
    pretty_assertions::assert_eq!(
        kamu.get_list_of_repo_aliases(&odf::DatasetRef::from(datasets[0].name.clone()))
            .await,
        [RepoAlias {
            dataset: odf::DatasetAlias::new(None, datasets[0].name.clone()),
            kind: "Pull".to_string(),
            alias: dataset_url.to_string(),
        }]
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_reports_progress_before_failed_iteration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reports_progress_before_failed_iteration(kamu: KamuCliPuppet) {
    add_data_files_dataset(&kamu, "test.pull-from-files").await;

    // One file per iteration; the third does not parse
    for (i, row) in ["A,1000", "B,2000", "C,not-a-number"].iter().enumerate() {
        let path = kamu.workspace_path().join(format!("data-{}.csv", i + 1));
        std::fs::write(path, format!("city,population\n{row}\n")).unwrap();
    }

    kamu.assert_failure_command_execution(
        ["pull", "test.pull-from-files"],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            r#"Ingested 3 block\(s\) with 2 record\(s\) before failing"#,
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_multiple_datasets_with_mixed_outcomes
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_multiple_datasets_with_mixed_outcomes(mut kamu: KamuCliPuppet) {
    let (alice, bob) = create_alice_and_bob(&kamu).await;

    std::fs::write(kamu.workspace_path().join("data.csv"), DATASET_INGEST_DATA).unwrap();
    let file_source_snapshot = DATASET_SNAPSHOT_STR.replace(
        "${{ env.data_dir || env.workspace_dir }}",
        &kamu.workspace_path().display().to_string(),
    );
    let add_dataset_with_visibility =
        async |kamu: &KamuCliPuppet, name: &str, snapshot: &str, visibility: &str| {
            kamu.execute_with_input(
                ["add", "--stdin", "--name", name, "--visibility", visibility],
                snapshot,
            )
            .await
            .success();
        };
    let add_dataset = async |kamu: &KamuCliPuppet, name: &str, snapshot: &str| {
        add_dataset_with_visibility(kamu, name, snapshot, "public").await;
    };

    kamu.set_account(Some(bob));
    add_dataset(&kamu, "b", &file_source_snapshot).await;
    add_dataset_with_visibility(&kamu, "p", &file_source_snapshot, "private").await;

    kamu.set_account(Some(alice));
    add_dataset(&kamu, "a", &file_source_snapshot).await;
    add_dataset(&kamu, "c", DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR).await;
    let gone_url = Url::from_directory_path(kamu.workspace_path().join("gone"))
        .unwrap()
        .join("c")
        .unwrap();
    kamu.execute([
        "repo",
        "alias",
        "add",
        "--pull",
        "alice/c",
        gone_url.as_str(),
    ])
    .await
    .success();

    // A dataset the account can read but not update keeps the permission error,
    // one it cannot read is reported as not found
    let gone_url_str = gone_url.as_str();
    kamu.assert_failure_command_execution(
        ["pull", "alice/a", "bob/b", "bob/p", "alice/c"],
        None,
        Some([
            r#"1 dataset\(s\) updated"#,
            r#"Failed to update 3 dataset\(s\)"#,
            regex::escape("User has no 'write' permission in dataset 'bob/b'").as_str(),
            regex::escape("Failed to pull bob/p: Dataset not found: bob/p").as_str(),
            regex::escape(&format!(
                "Failed to sync alice/c from {gone_url_str}: Dataset {gone_url_str}/ not found"
            ))
            .as_str(),
        ]),
    )
    .await;
    kamu.assert_success_command_execution(
        [
            "sql",
            "--output-format",
            "csv",
            "-c",
            r#"SELECT count(*) AS num_records FROM "alice/a""#,
        ],
        Some("num_records\n3"),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_set_watermark_keeps_source_state
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_set_watermark_keeps_source_state(kamu: KamuCliPuppet) {
    add_data_files_dataset(&kamu, "test.pull-from-files").await;
    std::fs::write(
        kamu.workspace_path().join("data-1.csv"),
        "city,population\nA,1000\n",
    )
    .unwrap();

    kamu.assert_success_command_execution(
        ["pull", "test.pull-from-files"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
    kamu.assert_success_command_execution(
        [
            "pull",
            "test.pull-from-files",
            "--set-watermark",
            "2051-01-02T03:04:05Z",
        ],
        None,
        Some(["Committed new block"]),
    )
    .await;

    // The file read before the watermark was set is not read again. Without a
    // source state the dataset would be uncacheable and a forced fetch would
    // read it once more
    kamu.assert_success_command_execution(
        ["pull", "test.pull-from-files", "--fetch-uncacheable"],
        None,
        Some([r#"1 dataset\(s\) up-to-date"#]),
    )
    .await;
    kamu.assert_success_command_execution(
        [
            "sql",
            "--output-format",
            "csv",
            "-c",
            r#"SELECT count(*) AS num_records FROM "test.pull-from-files""#,
        ],
        Some("num_records\n1"),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_datasets_sharing_a_fetch_step_keep_own_source_state
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_datasets_sharing_a_fetch_step_keep_own_source_state(kamu: KamuCliPuppet) {
    for dataset_name in ["test.first", "test.second"] {
        add_data_files_dataset(&kamu, dataset_name).await;
    }
    let write_file = |name: &str, city: &str| {
        std::fs::write(
            kamu.workspace_path().join(name),
            format!("city,population\n{city},1000\n"),
        )
        .unwrap();
    };

    write_file("data-1.csv", "A");
    kamu.assert_success_command_execution(
        ["pull", "test.first"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    // The first dataset reads only the new file, the second one reads both
    write_file("data-2.csv", "B");
    kamu.assert_success_command_execution(
        ["pull", "test.first", "test.second"],
        None,
        Some([r#"2 dataset\(s\) updated"#]),
    )
    .await;
    kamu.assert_success_command_execution(
        ["pull", "test.first", "test.second", "--fetch-uncacheable"],
        None,
        Some([r#"2 dataset\(s\) up-to-date"#]),
    )
    .await;

    for dataset_name in ["test.first", "test.second"] {
        kamu.assert_success_command_execution(
            [
                "sql",
                "--output-format",
                "csv",
                "-c",
                &format!(r#"SELECT city FROM "{dataset_name}" ORDER BY city"#),
            ],
            Some("city\nA\nB"),
            None::<Vec<&str>>,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_remote_by_id_fails
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_remote_by_id_fails(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.as_str();
    let repo_url = Url::from_directory_path(kamu.workspace_path().join("repo")).unwrap();
    kamu.execute(["repo", "add", "some-repo", repo_url.as_str()])
        .await
        .success();
    let remote_ref = format!(
        "some-repo/{}",
        odf::DatasetID::new_seeded_ed25519(b"remote").as_did_str()
    );

    kamu.assert_failure_command_execution(
        ["pull", &remote_ref],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            regex::escape(&format!(
                "Failed to sync dataset from {remote_ref}: Unable to infer a local name from \
                 {remote_ref}, please specify the destination explicitly"
            ))
            .as_str(),
        ]),
    )
    .await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute(["repo", "alias", "add", "--pull", dataset_name, &remote_ref])
        .await
        .success();

    kamu.assert_failure_command_execution(
        ["pull", dataset_name],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            regex::escape(&format!(
                "Failed to sync {dataset_name} from {remote_ref}: Syncing remote dataset by ID is \
                 not yet supported: {remote_ref}"
            ))
            .as_str(),
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_env_var_template_default_value(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_SNAPSHOT_STR,
        None,
        Some(["Added: test.pull-from-file", r#"Added 1 dataset\(s\)"#]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_INGEST_DATA).unwrap();

    kamu.assert_success_command_execution_with_env(
        ["pull", "test.pull-from-file"],
        vec![(
            OsStr::new("workspace_dir"),
            kamu.workspace_path().as_os_str(),
        )],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_env_var_template_default_value_missing_values(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_SNAPSHOT_STR,
        None,
        Some(["Added: test.pull-from-file", r#"Added 1 dataset\(s\)"#]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_INGEST_DATA).unwrap();

    kamu.assert_failure_command_execution(
        ["pull", "test.pull-from-file"],
        None,
        Some([
            r#"Failed to pull test.pull-from-file: Missing values for variable\(s\): 'env.data_dir \|\| env.workspace_dir'"#
        ]),
    )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A `VariableSet` labelled for the dataset supplies `env.data_dir` to a local
// `kamu pull`. Without the per-dataset resolution in `PullDatasetUseCaseImpl`
// the ingest map is empty and this fails exactly like
// `test_pull_env_var_template_default_value_missing_values` above -- the same
// dataset ingests fine through the server task path, which resolved env vars
// while the CLI did not.
async fn test_pull_env_var_from_labelled_variable_set(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_SNAPSHOT_STR,
        None,
        Some(["Added: test.pull-from-file", r#"Added 1 dataset\(s\)"#]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_INGEST_DATA).unwrap();

    // The label value must be the dataset's DID, so read it back after `add`.
    // It is the only dataset in this workspace.
    let datasets = kamu.list_datasets().await;
    pretty_assertions::assert_eq!(1, datasets.len(), "expected exactly one dataset");
    let dataset_id = datasets.into_iter().next().unwrap().id;

    let variable_set = indoc::formatdoc!(
        r#"
        $schema: https://opendatafabric.org/schemas/config/v1alpha1/VariableSet
        headers:
          name: pull-vars
          labels:
            legacy-config-target-dataset: {dataset_id}
        spec:
          variables:
            data_dir: {data_dir}
        "#,
        data_dir = kamu.workspace_path().display(),
    );

    kamu.assert_success_command_execution_with_input(
        ["apply", "--stdin"],
        variable_set,
        None,
        Some([r#"Created: STDIN -> VariableSet/pull-vars"#]),
    )
    .await;

    // No `--env`: the value can only come from the labelled VariableSet.
    kamu.assert_success_command_execution(
        ["pull", "test.pull-from-file"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_reset_derivative(kamu: KamuCliPuppet) {
    let dataset_alias = odf::DatasetAlias::new(None, DATASET_ROOT_PLAYER_NAME.clone());
    let dataset_derivative_alias =
        odf::DatasetAlias::new(None, DATASET_DERIVATIVE_LEADERBOARD_NAME.clone());

    add_player_scores_with_derivative(&kamu).await;

    kamu.assert_success_command_execution(
        ["pull", dataset_derivative_alias.dataset_name.as_str()],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    let expected_derivative_schema = indoc::indoc!(
        r#"
        message arrow_schema {
          REQUIRED INT64 offset;
          REQUIRED INT32 op;
          REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
          REQUIRED INT64 match_time (TIMESTAMP(MILLIS,true));
          REQUIRED INT64 place (INTEGER(64,false));
          REQUIRED INT64 match_id;
          REQUIRED BYTE_ARRAY player_id (STRING);
          REQUIRED INT64 score;
        }
        "#
    );
    let expected_derivative_data = indoc::indoc!(
        r#"
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | offset | op | system_time          | match_time           | place | match_id | player_id | score |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        "#
    );
    kamu.assert_last_data_slice(
        &dataset_derivative_alias,
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;

    // Compact root dataset
    kamu.execute([
        "--yes",
        "system",
        "compact",
        dataset_alias.dataset_name.as_str(),
        "--hard",
        "--keep-metadata-only",
    ])
    .await
    .success();

    // Pull derivative should fail
    kamu.assert_failure_command_execution(
        ["pull", dataset_derivative_alias.dataset_name.as_str()],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            r#"Failed to pull ([^ ]+/)?leaderboard: Invalid block interval .* in input dataset ([^ ]+/)?player-scores"#,
        ]),
    )
    .await;

    // Add new data to root dataset
    kamu.ingest_data(
        &dataset_alias.dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "pull",
            dataset_derivative_alias.dataset_name.as_str(),
            "--reset-derivatives-on-diverged-input",
        ],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    let expected_derivative_data = indoc::indoc!(
        r#"
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | offset | op | system_time          | match_time           | place | match_id | player_id | score |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 1     | 2        | Charlie   | 90    |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Alice     | 70    |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        "#
    );
    kamu.assert_last_data_slice(
        &dataset_derivative_alias,
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;

    // Compacting the root again diverges the derivative once more; with no new
    // input data, the reset leaves the derivative empty
    kamu.execute([
        "--yes",
        "system",
        "compact",
        dataset_alias.dataset_name.as_str(),
        "--hard",
        "--keep-metadata-only",
    ])
    .await
    .success();
    kamu.assert_success_command_execution(
        [
            "pull",
            dataset_derivative_alias.dataset_name.as_str(),
            "--reset-derivatives-on-diverged-input",
        ],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
    pretty_assertions::assert_eq!(
        0,
        kamu.list_datasets()
            .await
            .into_iter()
            .find(|dataset| dataset.name == dataset_derivative_alias.dataset_name)
            .unwrap()
            .records
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_derivative(kamu: KamuCliPuppet) {
    let dataset_derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.clone();

    add_player_scores_with_derivative(&kamu).await;

    kamu.assert_success_command_execution(
        [
            "tail",
            dataset_derivative_name.as_str(),
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌┐
            ││
            ├┤
            ││
            └┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["pull", dataset_derivative_name.as_str()],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    kamu.assert_player_scores_dataset_data(indoc::indoc!(
        r#"
        ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
        │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
        ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
        │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
        │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
        └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
        "#
    ))
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_set_watermark(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        [
            "pull",
            dataset_name.as_str(),
            "--set-watermark",
            "2051-01-02T03:04:05Z",
        ],
        None,
        Some(["Committed new block"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_uncacheable_source
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_uncacheable_source(kamu: KamuCliPuppet) {
    // A source without `ETag` or `Last-Modified` records no source state, so
    // once it has data it is uncacheable
    let source = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/data.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("city,population\nA,1000\n"))
        .mount(&source)
        .await;

    let snapshot = city_population_root_snapshot(
        "test.pull-uncacheable",
        &format!(r#"{{ kind: Url, url: "{}/data.csv" }}"#, source.uri()),
    );
    kamu.execute_with_input(["add", "--stdin"], snapshot)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["pull", "test.pull-uncacheable"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    source.reset().await;
    Mock::given(method("GET"))
        .and(path("/data.csv"))
        .respond_with(ResponseTemplate::new(200).set_body_string("city,population\nB,2000\n"))
        .mount(&source)
        .await;
    kamu.assert_success_command_execution(
        ["pull", "test.pull-uncacheable"],
        None,
        Some([r#"1 dataset\(s\) up-to-date"#]),
    )
    .await;
    kamu.assert_success_command_execution(
        ["pull", "test.pull-uncacheable", "--fetch-uncacheable"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_after_inputs_changed_fails
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_after_inputs_changed_fails(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(&kamu).await;
    kamu.execute(["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()])
        .await
        .success();

    let other_name = odf::DatasetName::new_unchecked("other-scores");
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR
            .replace("name: player-scores", "name: other-scores"),
    )
    .await
    .success();
    kamu.ingest_data(
        &other_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;
    commit_event(
        &kamu,
        &DATASET_DERIVATIVE_LEADERBOARD_NAME,
        &leaderboard_set_transform_event(
            &dataset_id(&kamu, &other_name).await,
            DATASET_DERIVATIVE_LEADERBOARD_QUERY,
        ),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([
            "Failed to pull leaderboard: Transform of leaderboard changed its inputs since it \
             last ran, which is not supported yet",
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_leaves_no_working_files
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_leaves_no_working_files(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(&kamu).await;

    kamu.assert_success_command_execution(
        ["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    let run_dir = kamu.workspace_path().join(".kamu").join("run");
    pretty_assertions::assert_eq!(
        Vec::<std::path::PathBuf>::new(),
        std::fs::read_dir(run_dir)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Adds a root dataset that appends the workspace's `data-*.csv` files of
/// cities and their populations
async fn add_data_files_dataset(kamu: &KamuCliPuppet, dataset_name: &str) {
    let snapshot = city_population_root_snapshot(
        dataset_name,
        &format!(
            r#"{{ kind: FilesGlob, path: "{}/data-*.csv" }}"#,
            kamu.workspace_path().display()
        ),
    );
    kamu.execute_with_input(["add", "--stdin"], snapshot)
        .await
        .success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
