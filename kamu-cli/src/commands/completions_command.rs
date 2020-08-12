use super::{Command, Error};

const BASH_COMPLETIONS: &str = "
_kamu_rs_()
{
    _COMP_OUTPUTSTR=\"$( kamu-rs complete -- \"${COMP_WORDS[*]}\" ${COMP_CWORD} )\"
    if test $? -ne 0; then
        return 1
    fi
    readarray -t COMPREPLY < <( echo -n \"$_COMP_OUTPUTSTR\" )
}

complete -F _kamu_rs_ kamu-rs
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

    fn run(&mut self) -> Result<(), Error> {
        // TODO: Remove once clap allows to programmatically complete values
        // See: https://github.com/clap-rs/clap/issues/568
        match self.shell {
            clap::Shell::Bash => print!("{}", BASH_COMPLETIONS),
            _ => self
                .app
                .gen_completions_to("kamu-rs", self.shell, &mut std::io::stdout()),
        };
        Ok(())
    }
}
