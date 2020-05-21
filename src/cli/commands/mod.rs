mod log_command;
mod pull_command;
mod sql_server_command;

pub use log_command::LogCommand;
pub use pull_command::PullCommand;
pub use sql_server_command::SqlServerCommand;

pub trait Command {
    fn run(&mut self);
}

pub struct NoOpCommand;

impl Command for NoOpCommand {
    fn run(&mut self) {}
}
