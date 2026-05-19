// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::example_ws::ExampleWorkspace;

#[test_group::group(examples)]
#[test]
fn test_example_leaderboard() {
    let ws = ExampleWorkspace::new_by_name_with_clean_state("leaderboard");

    std::process::Command::new("kamu")
        .args(["add", ".", "--recursive"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    let out = std::process::Command::new("kamu")
        .args(["list", "--output-format", "json"])
        .current_dir(&ws.root)
        .output()
        .unwrap()
        .exit_ok()
        .unwrap();

    let out: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();

    pretty_assertions::assert_eq!(
        out,
        serde_json::json!([
            {
                "Kind": "Derivative",
                "Name": "leaderboard",
                "Pulled": null,
                "Records": 0,
                "Size": 0,
            },
            {
                "Kind": "Root",
                "Name": "player-scores",
                "Pulled": null,
                "Records": 0,
                "Size": 0,
            },
        ])
    );

    // Round 1

    std::process::Command::new("kamu")
        .args(["ingest", "player-scores", "./data/1.ndjson"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    std::process::Command::new("kamu")
        .args(["pull", "leaderboard"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    let out = std::process::Command::new("kamu")
        .args(["list", "--output-format", "json"])
        .current_dir(&ws.root)
        .output()
        .unwrap()
        .exit_ok()
        .unwrap();

    let mut out: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    out[0]["Pulled"] = serde_json::Value::Null;
    out[1]["Pulled"] = serde_json::Value::Null;
    out[0]["Size"] = serde_json::Value::Null;
    out[1]["Size"] = serde_json::Value::Null;

    pretty_assertions::assert_eq!(
        out,
        serde_json::json!([
            {
                "Kind": "Derivative",
                "Name": "leaderboard",
                "Pulled": null,
                "Records": 2,
                "Size": null,
            },
            {
                "Kind": "Root",
                "Name": "player-scores",
                "Pulled": null,
                "Records": 2,
                "Size": null,
            },
        ])
    );

    // Round 2

    std::process::Command::new("kamu")
        .args(["ingest", "player-scores", "./data/2.ndjson"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    std::process::Command::new("kamu")
        .args(["pull", "leaderboard"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    // Round 3

    std::process::Command::new("kamu")
        .args(["ingest", "player-scores", "./data/3.ndjson"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    std::process::Command::new("kamu")
        .args(["pull", "leaderboard"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    let out = std::process::Command::new("kamu")
        .args([
            "sql",
            "-c",
            "select offset, op, place, match_id, player_id, score from leaderboard order by offset",
            "--output-format",
            "json",
        ])
        .current_dir(&ws.root)
        .output()
        .unwrap()
        .exit_ok()
        .unwrap();

    let out: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();

    pretty_assertions::assert_eq!(
        out,
        serde_json::json!(
            [
                {
                    "offset": 0,
                    "op": 0,
                    "match_id": 1,
                    "place": 1,
                    "player_id": "Alice",
                    "score": 100,
                },
                {
                    "offset": 1,
                    "op": 0,
                    "match_id": 1,
                    "place": 2,
                    "player_id": "Bob",
                    "score": 80,
                },
                {
                    "offset": 2,
                    "op": 1,
                    "match_id": 1,
                    "place": 2,
                    "player_id": "Bob",
                    "score": 80,
                },
                {
                    "offset": 3,
                    "op": 0,
                    "match_id": 2,
                    "place": 2,
                    "player_id": "Charlie",
                    "score": 90,
                },
                {
                    "offset": 4,
                    "op": 1,
                    "match_id": 1,
                    "place": 1,
                    "player_id": "Alice",
                    "score": 100,
                },
                {
                    "offset": 5,
                    "op": 1,
                    "match_id": 2,
                    "place": 2,
                    "player_id": "Charlie",
                    "score": 90,
                },
                {
                    "offset": 6,
                    "op": 0,
                    "match_id": 3,
                    "place": 1,
                    "player_id": "Charlie",
                    "score": 110,
                },
                {
                    "offset": 7,
                    "op": 0,
                    "match_id": 1,
                    "place": 2,
                    "player_id": "Alice",
                    "score": 100,
                },
            ]
        )
    );
}
