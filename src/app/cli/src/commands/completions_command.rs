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
    COMPREPLY=()
    if test -n \"$_COMP_OUTPUTSTR\"; then
        while IFS= read -r _KAMU_COMP_CANDIDATE; do
            COMPREPLY[${#COMPREPLY[@]}]=\"$_KAMU_COMP_CANDIDATE\"
        done <<< \"$_COMP_OUTPUTSTR\"
    fi
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

#[cfg(test)]
mod tests {
    use std::process::Command as StdCommand;

    use super::BASH_COMPLETIONS;

    #[test]
    fn bash_completions_preserve_whitespace_in_candidates() {
        let completions = BashCompletionHarness::complete_with_special_characters();

        assert_eq!(completions, ["my prod", "other", "*.txt"]);
    }

    struct BashCompletionHarness;

    impl BashCompletionHarness {
        fn complete_with_special_characters() -> Vec<String> {
            // Disable `mapfile` to model macOS's system Bash 3.2.
            let script = format!(
                "enable -n mapfile 2>/dev/null || true\nkamu() {{ printf '%s\\n' 'my prod' \
                 'other' '*.txt'; }}\n{BASH_COMPLETIONS}\nCOMP_WORDS=(kamu context \
                 use)\nCOMP_CWORD=2\n_kamu_\nprintf '%s\\n' \"${{COMPREPLY[@]}}\"\n"
            );

            let output = StdCommand::new("bash")
                .arg("-c")
                .arg(&script)
                .output()
                .expect("failed to run bash");

            assert!(output.status.success());
            String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(ToOwned::to_owned)
                .collect()
        }
    }
}
