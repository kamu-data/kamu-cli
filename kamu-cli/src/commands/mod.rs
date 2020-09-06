pub use super::error::Error;

mod add_command;
pub use add_command::*;

mod complete_command;
pub use complete_command::*;

mod completions_command;
pub use completions_command::*;

mod delete_command;
pub use delete_command::*;

mod depgraph_command;
pub use depgraph_command::*;

mod list_command;
pub use list_command::*;

mod log_command;
pub use log_command::*;

mod init_command;
pub use init_command::*;

mod new_dataset_command;
pub use new_dataset_command::*;

mod notebook_command;
pub use notebook_command::*;

mod pull_command;
pub use pull_command::*;

mod pull_images_command;
pub use pull_images_command::*;

mod set_watermark_command;
pub use set_watermark_command::*;

mod sql_server_command;
pub use sql_server_command::*;

mod sql_shell_command;
pub use sql_shell_command::*;

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
