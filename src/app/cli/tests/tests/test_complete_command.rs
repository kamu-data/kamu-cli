// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::io::{ErrorKind, Write};
use std::sync::Arc;

use dill::Component;
use kamu_cli::commands::*;
use kamu_cli::config::ConfigService;
use kamu_cli::{CLIError, WorkspaceLayout, WorkspaceService};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct FailingWriter(ErrorKind);

impl Write for FailingWriter {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        Err(self.0.into())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Err(self.0.into())
    }
}

fn complete_command(input: &str, current: usize) -> Arc<CompleteCommand> {
    let catalog = dill::Catalog::builder()
        .add_value(WorkspaceLayout::new("."))
        .add::<ConfigService>()
        .add_builder(WorkspaceService::builder().with_multi_tenant(false))
        .add_builder(CompleteCommand::builder(input.to_string(), current))
        .build();

    catalog.get_one().unwrap()
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

#[test_log::test(tokio::test)]
async fn test_write_errors_are_propagated() {
    // The completion helpers used to `unwrap()` their writes, turning any output
    // error into a panic. Config keys are the cheapest helper to reach that does
    // not need a dataset registry
    let err = complete_command("kamu config set engine.runt", 3)
        .complete(&mut FailingWriter(ErrorKind::PermissionDenied))
        .await
        .unwrap_err();

    // `.int_err()` is what the pre-existing writes in `complete()` already use, so
    // a write error surfaces as a critical failure rather than a panic
    let debug = format!("{err:?}");
    assert!(debug.contains("PermissionDenied"), "{debug}");
    assert_matches!(err, CLIError::CriticalFailure { .. });
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
