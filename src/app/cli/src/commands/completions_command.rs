// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{ErrorKind, Write};

use clap::CommandFactory as _;

use super::{CLIError, Command};

const BASH_COMPLETIONS: &str = "
_kamu_()
{
    _COMP_OUTPUTSTR=\"$( kamu complete -- \"${COMP_WORDS[*]}\" ${COMP_CWORD} )\"
    if test $? -ne 0; then
        return 1
    fi
    COMPREPLY=($( echo -n \"$_COMP_OUTPUTSTR\" ))
}

complete -F _kamu_ kamu
";

#[dill::component]
#[dill::interface(dyn Command)]

pub struct CompletionsCommand {
    #[dill::component(explicit)]
    shell: clap_complete::Shell,
}

impl CompletionsCommand {
    pub fn write_completions(&self, output: &mut impl Write) -> Result<(), CLIError> {
        // TODO: Remove once clap allows to programmatically complete values
        // See: https://github.com/clap-rs/clap/issues/568
        let mut cli = crate::cli::Cli::command();
        let bin_name = cli.get_name().to_owned();

        // Note: the script is rendered into a buffer first, as
        // `clap_complete::generate()` panics on writer errors. The fallible
        // `Generator::try_generate()` would avoid this, but requires replicating
        // `set_bin_name()` + `build()` by hand
        let mut script = Vec::new();
        match self.shell {
            clap_complete::Shell::Bash => script.extend_from_slice(BASH_COMPLETIONS.as_bytes()),
            _ => clap_complete::generate(self.shell, &mut cli, bin_name, &mut script),
        }

        match output.write_all(&script) {
            // Consumers may exit before reading all output, e.g. `source <(kamu completions bash)`
            Err(err) if err.kind() == ErrorKind::BrokenPipe => Ok(()),
            res => Ok(res?),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompletionsCommand {
    async fn run(&self) -> Result<(), CLIError> {
        self.write_completions(&mut std::io::stdout())
    }
}
