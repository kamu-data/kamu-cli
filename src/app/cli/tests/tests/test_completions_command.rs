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
fn test_broken_pipe_is_ignored() {
    // Note: every shell other than Bash is rendered by `clap_complete::generate()`,
    // which panics on writer errors, so this also pins the decision to render the
    // script into a buffer first
    for shell in clap_complete::Shell::value_variants() {
        completions_command(*shell)
            .write_completions(&mut FailingWriter(ErrorKind::BrokenPipe))
            .unwrap();
    }
}

#[test_log::test]
fn test_other_write_errors_are_propagated() {
    let res = completions_command(clap_complete::Shell::Bash)
        .write_completions(&mut FailingWriter(ErrorKind::PermissionDenied));

    assert_matches!(
        res,
        Err(CLIError::Failure { source, .. })
            if source.downcast_ref::<Error>().map(Error::kind) == Some(ErrorKind::PermissionDenied)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
