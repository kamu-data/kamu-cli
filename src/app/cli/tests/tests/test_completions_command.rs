// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::io::{Error, ErrorKind, Write};
use std::sync::Arc;

use clap::ValueEnum as _;
use dill::TypedBuilder;
use kamu_cli::CLIError;
use kamu_cli::commands::*;

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

fn completions_command(shell: clap_complete::Shell) -> Arc<CompletionsCommand> {
    CompletionsCommand::builder(shell)
        .get(&dill::Catalog::builder().build())
        .unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test]
fn test_writes_completions() {
    for (shell, expected) in [
        (clap_complete::Shell::Bash, "complete -F _kamu_ kamu"),
        (clap_complete::Shell::Zsh, "#compdef kamu"),
        (clap_complete::Shell::Fish, "complete -c kamu"),
    ] {
        let mut output = Vec::new();
        completions_command(shell)
            .write_completions(&mut output)
            .unwrap();

        let script = String::from_utf8(output).unwrap();
        assert!(script.contains(expected), "{shell}: {script}");
    }
}

#[test_log::test]
fn test_write_errors_are_propagated() {
    // Note: only Bash is covered here. Every other shell is rendered by
    // `clap_complete::generate()`, which panics rather than returning writer
    // errors, and its fallible `try_generate()` is no escape either - `Fish`
    // still panics internally (`clap_complete-4.6.9` `fish.rs:277`). Broken
    // pipes are handled a level up, by the `pipecheck` writer `run()` passes in
    // - see the test below
    let res = completions_command(clap_complete::Shell::Bash)
        .write_completions(&mut FailingWriter(ErrorKind::PermissionDenied));

    assert_matches!(
        res,
        Err(CLIError::Failure { source, .. })
            if source.downcast_ref::<Error>().map(Error::kind) == Some(ErrorKind::PermissionDenied)
    );
}

// A broken pipe takes the whole process down, so unlike the cases above it can
// only be observed from the outside
#[cfg(unix)]
#[test_log::test]
fn test_broken_pipe_terminates_by_sigpipe() {
    use std::os::unix::process::ExitStatusExt as _;

    // Run outside any `.kamu` that happens to sit above the checkout
    let workdir = tempfile::tempdir().unwrap();

    for shell in clap_complete::Shell::value_variants() {
        let (reader, writer) = std::io::pipe().unwrap();

        // Closing the read end before spawning makes the very first write fail, so the
        // test does not depend on the child being scheduled before or after this point
        drop(reader);

        let output = std::process::Command::new(env!("CARGO_BIN_EXE_kamu-cli"))
            .args(["completions", &shell.to_string()])
            .current_dir(workdir.path())
            .stdout(writer)
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap()
            .wait_with_output()
            .unwrap();

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("panicked") && !stderr.contains("has crashed"),
            "{shell}: {stderr}"
        );
        assert_eq!(
            output.status.signal(),
            Some(libc::SIGPIPE),
            "{shell}: {:?}, stderr: {stderr}",
            output.status
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
