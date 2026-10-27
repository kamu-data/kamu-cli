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
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    leaderboard_set_transform_event,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

use crate::utils::{commit_event, dataset_id};

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
// test_pull_derivative_of_never_pulled_root
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_of_never_pulled_root(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    let derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str();
    let expected_error = format!(
        "Failed to pull {derivative_name}: Input dataset {} has no data yet, pull it first",
        DATASET_ROOT_PLAYER_NAME.as_str(),
    );

    kamu.assert_failure_command_execution(
        ["pull", derivative_name],
        None,
        Some([
            r#"Failed to update 1 dataset\(s\)"#,
            regex::escape(&expected_error).as_str(),
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_reports_progress_before_failed_iteration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reports_progress_before_failed_iteration(kamu: KamuCliPuppet) {
    let snapshot = format!(
        indoc::indoc!(
            r#"
            kind: DatasetSnapshot
            version: 1
            content:
              name: test.pull-from-files
              kind: Root
              metadata:
                - kind: SetPollingSource
                  fetch:
                    kind: FilesGlob
                    path: {}/data-*.csv
                  read:
                    kind: Csv
                    header: true
                    schema:
                      fields:
                        - name: city
                          type: String
                        - name: population
                          type: Int64
                  merge:
                    kind: append
            "#
        ),
        kamu.workspace_path().display(),
    );
    kamu.execute_with_input(["add", "--stdin"], snapshot)
        .await
        .success();

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

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.ingest_data(
        &dataset_alias.dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

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
        Some([r#"Failed to update 1 dataset\(s\)"#]),
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_pull_derivative(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();
    let dataset_derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

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
    let data_path = kamu.workspace_path().join("data.csv");
    let snapshot = indoc::formatdoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: test.pull-uncacheable
          kind: Root
          metadata:
            - kind: SetPollingSource
              fetch:
                kind: Url
                url: file://{data_path}
              read:
                kind: Csv
                header: true
                schema:
                  fields:
                    - name: city
                      type: String
                    - name: population
                      type: Int64
              merge:
                kind: append
        "#,
        data_path = data_path.display(),
    );
    kamu.execute_with_input(["add", "--stdin"], snapshot)
        .await
        .success();

    std::fs::write(&data_path, "city,population\nA,1000\n").unwrap();
    kamu.assert_success_command_execution(
        ["pull", "test.pull-uncacheable"],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;

    // The watermark block records no source state, so the source becomes
    // uncacheable
    kamu.assert_success_command_execution(
        [
            "pull",
            "test.pull-uncacheable",
            "--set-watermark",
            "2050-01-01T00:00:00Z",
        ],
        None,
        Some(["Committed new block"]),
    )
    .await;

    std::fs::write(&data_path, "city,population\nB,2000\n").unwrap();
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
// test_pull_derivative_with_engine_name_in_any_case
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_with_engine_name_in_any_case(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(
        &kamu,
        &DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .replace("engine: datafusion", "engine: DataFusion"),
    )
    .await;

    kamu.assert_success_command_execution(
        ["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"1 dataset\(s\) updated"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_with_unknown_engine_fails
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_with_unknown_engine_fails(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(
        &kamu,
        &DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .replace("engine: datafusion", "engine: no-such-engine"),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some(["Failed to pull leaderboard: Unsupported engine no-such-engine"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_after_inputs_changed_fails
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_after_inputs_changed_fails(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(&kamu, DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR).await;
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
            "SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS place, match_time, \
             match_id, player_id, score FROM player_scores LIMIT 2",
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
// test_pull_reset_derivative_without_new_input_data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reset_derivative_without_new_input_data(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(&kamu, DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR).await;
    kamu.execute(["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()])
        .await
        .success();
    kamu.execute([
        "--yes",
        "system",
        "compact",
        DATASET_ROOT_PLAYER_NAME.as_str(),
        "--hard",
        "--keep-metadata-only",
    ])
    .await
    .success();

    kamu.assert_failure_command_execution(
        ["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"Failed to pull leaderboard: Invalid block interval .* in input dataset player-scores"#]),
    )
    .await;

    // The reset leaves the derivative without data, as its input has none
    kamu.assert_success_command_execution(
        [
            "pull",
            DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str(),
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
            .find(|dataset| dataset.name == *DATASET_DERIVATIVE_LEADERBOARD_NAME)
            .unwrap()
            .records
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative_leaves_no_working_files
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative_leaves_no_working_files(kamu: KamuCliPuppet) {
    add_player_scores_with_derivative(&kamu, DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR).await;

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

/// Adds the root with its first chunk of data, and a derivative of it
async fn add_player_scores_with_derivative(kamu: &KamuCliPuppet, derivative_snapshot: &str) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute_with_input(["add", "--stdin"], derivative_snapshot)
        .await
        .success();
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
