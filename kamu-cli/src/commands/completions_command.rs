// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

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

pub struct CompletionsCommand {
    app: clap::App<'static, 'static>,
    shell: clap::Shell,
}

impl CompletionsCommand {
    pub fn new(app: clap::App<'static, 'static>, shell: clap::Shell) -> Self {
        Self {
            app: app,
            shell: shell,
        }
    }
}

impl Command for CompletionsCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), CLIError> {
        // TODO: Remove once clap allows to programmatically complete values
        // See: https://github.com/clap-rs/clap/issues/568
        let program = self.app.get_name().to_owned();
        match self.shell {
            clap::Shell::Bash => print!("{}", BASH_COMPLETIONS),
            _ => self
                .app
                .gen_completions_to(&program, self.shell, &mut std::io::stdout()),
        };
        Ok(())
    }
}
