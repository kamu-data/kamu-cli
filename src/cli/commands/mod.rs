pub use super::error::Error;

mod complete_command;
pub use complete_command::CompleteCommand;

mod completions_command;
pub use completions_command::CompletionsCommand;

mod list_command;
pub use list_command::ListCommand;

mod log_command;
pub use log_command::LogCommand;

mod init_command;
pub use init_command::InitCommand;

mod pull_command;
pub use pull_command::PullCommand;

mod sql_server_command;
pub use sql_server_command::SqlServerCommand;

mod sql_shell_command;
pub use sql_shell_command::SqlShellCommand;

pub trait Command {
    fn needs_workspace(&self) -> bool {
        true
    }

    fn run(&mut self) -> Result<(), Error>;
}

pub struct NoOpCommand;

impl Command for NoOpCommand {
    fn run(&mut self) -> Result<(), Error> {
        Ok(())
    }
}
