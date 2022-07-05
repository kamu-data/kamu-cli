// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

pub use super::error::*;

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

mod inspect_schema_command;
pub use inspect_schema_command::*;

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

mod rename_command;
pub use rename_command::*;

mod repository_add_command;
pub use repository_add_command::*;

mod repository_delete_command;
pub use repository_delete_command::*;

mod repository_list_command;
pub use repository_list_command::*;

mod reset_command;
pub use reset_command::*;

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

mod system_api_server_gql_query_command;
pub use system_api_server_gql_query_command::*;

mod system_api_server_gql_schema_command;
pub use system_api_server_gql_schema_command::*;

mod system_api_server_run_command;
pub use system_api_server_run_command::*;

mod system_ipfs_add_command;
pub use system_ipfs_add_command::*;

mod ui_command;
pub use ui_command::*;

mod verify_command;
pub use verify_command::*;

#[async_trait::async_trait(?Send)]
pub trait Command {
    fn needs_workspace(&self) -> bool {
        true
    }

    async fn run(&mut self) -> Result<(), CLIError>;
}

pub struct NoOpCommand;

#[async_trait::async_trait(?Send)]
impl Command for NoOpCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        Ok(())
    }
}
