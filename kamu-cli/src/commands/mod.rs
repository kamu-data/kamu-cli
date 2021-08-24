pub use super::error::CLIError;

mod common;

mod add_command;
pub use add_command::*;

mod alias_add_command;
pub use alias_add_command::*;

mod alias_delete_command;
pub use alias_delete_command::*;

mod alias_list_command;
pub use alias_list_command::*;

mod complete_command;
pub use complete_command::*;

mod completions_command;
pub use completions_command::*;

mod config_command;
pub use config_command::*;

mod delete_command;
pub use delete_command::*;

mod init_command;
pub use init_command::*;

mod inspect_query_command;
pub use inspect_query_command::*;

mod list_command;
pub use list_command::*;

mod lineage_command;
pub use lineage_command::*;

mod log_command;
pub use log_command::*;

mod new_dataset_command;
pub use new_dataset_command::*;

mod notebook_command;
pub use notebook_command::*;

mod pull_command;
pub use pull_command::*;

mod pull_images_command;
pub use pull_images_command::*;

mod push_command;
pub use push_command::*;

mod repository_add_command;
pub use repository_add_command::*;

mod repository_delete_command;
pub use repository_delete_command::*;

mod repository_list_command;
pub use repository_list_command::*;

mod search_command;
pub use search_command::*;

mod set_watermark_command;
pub use set_watermark_command::*;

mod sql_server_command;
pub use sql_server_command::*;

mod sql_server_livy_command;
pub use sql_server_livy_command::*;

mod sql_shell_command;
pub use sql_shell_command::*;

mod tail_command;
pub use tail_command::*;

mod verify_command;
pub use verify_command::*;

pub trait Command {
    fn needs_workspace(&self) -> bool {
        true
    }

    fn run(&mut self) -> Result<(), CLIError>;
}

pub struct NoOpCommand;

impl Command for NoOpCommand {
    fn run(&mut self) -> Result<(), CLIError> {
        Ok(())
    }
}
