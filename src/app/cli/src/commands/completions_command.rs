// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

#[async_trait::async_trait(?Send)]
impl Command for CompletionsCommand {
    async fn run(&self) -> Result<(), CLIError> {
        // TODO: Remove once clap allows to programmatically complete values
        // See: https://github.com/clap-rs/clap/issues/568
        let mut cli = crate::cli::Cli::command();
        let bin_name = cli.get_name().to_owned();
        match self.shell {
            clap_complete::Shell::Bash => print!("{BASH_COMPLETIONS}"),
            _ => {
                clap_complete::generate(self.shell, &mut cli, bin_name, &mut std::io::stdout());
            }
        }
        Ok(())
    }
}
