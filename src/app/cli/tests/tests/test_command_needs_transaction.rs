// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser;
use kamu_cli::cli::Cli;
use kamu_cli::command_needs_transaction;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const BLOCK_HASH: &str = "f16208e1d101a0464b9312a75a8a455cf81b81d92ca265285e61047a35bbc40577e4b";

#[test]
fn test_command_needs_transaction() {
    let cases: &[(&[&str], bool)] = &[
        // Commands that open their own transactions, if any, rather than run in one
        (&["system", "compact", "ds"], false),
        (&["system", "diagnose"], false),
        (&["system", "ipfs", "add", "ds"], false),
        (&["reset", "ds", BLOCK_HASH], false),
        (&["verify", "ds"], false),
        (&["pull", "ds"], false),
        (&["push", "ds"], false),
        (&["apply", "file.yaml"], false),
        (&["ui"], false),
        (&["login"], false),
        (&["system", "api-server"], false),
        (&["context", "list"], false),
        // Commands that run in one transaction
        (&["list"], true),
        (&["log", "ds"], true),
        (&["delete", "ds"], true),
        (
            &["pull", "ds", "--set-watermark", "2020-01-01T00:00:00Z"],
            true,
        ),
        (
            &["system", "api-server", "gql-query", "{ apiVersion }"],
            true,
        ),
        (&["system", "info"], true),
        (&["system", "gc"], true),
        (&["context", "api-resources"], true),
    ];

    for (args, expected) in cases {
        let cli = Cli::try_parse_from(std::iter::once("kamu").chain(args.iter().copied()))
            .unwrap_or_else(|e| panic!("`{}` does not parse: {e}", args.join(" ")));
        assert_eq!(
            command_needs_transaction(&cli),
            *expected,
            "kamu {}",
            args.join(" ")
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
