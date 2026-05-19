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
fn test_example_trading() {
    let ws = ExampleWorkspace::new_by_name_with_clean_state("trading");

    std::process::Command::new("kamu")
        .args(["add", ".", "--recursive"])
        .current_dir(&ws.root)
        .status()
        .unwrap()
        .exit_ok()
        .unwrap();

    std::process::Command::new("kamu")
        .args(["pull", "--all"])
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
    if let serde_json::Value::Array(out) = &mut out {
        for i in out {
            if let serde_json::Value::Object(i) = i {
                i.remove("Pulled");
                i.remove("Size");
            }
        }
    }

    pretty_assertions::assert_eq!(
        out,
        serde_json::json!(
            [
                {
                    "Kind": "Root",
                    "Name": "com.yahoo.finance.tickers.daily",
                    "Records": 2384,
                },
                {
                    "Kind": "Derivative",
                    "Name": "my.trading.holdings",
                    "Records": 48,
                },
                {
                    "Kind": "Derivative",
                    "Name": "my.trading.holdings.market-value",
                    "Records": 986,
                },
                {
                    "Kind": "Root",
                    "Name": "my.trading.transactions",
                    "Records": 48,
                },
            ]
        )
    );
}
