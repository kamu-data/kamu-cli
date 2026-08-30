// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::TypedBuilder;
use kamu_cli::commands::*;
use kamu_cli::config::ConfigService;
use kamu_cli::{CLIError, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn complete_command(input: &str, current: usize) -> Arc<CompleteCommand> {
    let catalog = dill::Catalog::builder()
        .add_value(ConfigService::new(&WorkspaceLayout::new(".")))
        .build();

    CompleteCommand::builder(input.to_owned(), current)
        .get(&catalog)
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_completes_config_keys() {
    let mut output = Vec::new();
    complete_command("kamu config set engine.runt", 3)
        .complete(&mut output)
        .await
        .unwrap();

    assert_eq!(String::from_utf8(output).unwrap(), "engine.runtime\n");
}

// A broken pipe takes the whole process down, so unlike the cases above it can
// only be observed from the outside
#[cfg(unix)]
#[test_log::test]
fn test_broken_pipe_terminates_by_sigpipe() {
    use std::os::unix::process::ExitStatusExt as _;

    // Run outside any `.kamu` that happens to sit above the checkout
    let workdir = tempfile::tempdir().unwrap();

    // One case per completion path: a subcommand, a positional handled by a helper,
    // and an option value, which returns early and so flushes on its own line
    for (input, current) in [
        ("kamu l", 1),
        ("kamu config set engine.runt", 3),
        ("kamu --", 1),
    ] {
        let (reader, writer) = std::io::pipe().unwrap();

        // Closing the read end before spawning makes the very first write fail, so the
        // test does not depend on the child being scheduled before or after this point
        drop(reader);

        let output = std::process::Command::new(env!("CARGO_BIN_EXE_kamu-cli"))
            .args(["complete", input, &current.to_string()])
            .current_dir(workdir.path())
            .stdout(writer)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .unwrap();

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("panicked") && !stderr.contains("Internal error"),
            "{input:?}: {stderr}"
        );
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGPIPE),
            "{input:?}: {:?}, stderr: {stderr}",
            output.status
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
