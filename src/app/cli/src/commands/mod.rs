// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod add_command;
mod alias_add_command;
mod alias_delete_command;
mod alias_list_command;
mod common;
mod compact_command;
mod complete_command;
mod completions_command;
mod config_command;
mod delete_command;
mod export_command;
mod gc_command;
mod ingest_command;
mod init_command;
mod inspect_lineage_command;
mod inspect_query_command;
mod inspect_schema_command;
mod list_command;
mod log_command;
mod login_command;
mod login_silent_command;
mod logout_command;
mod new_dataset_command;
mod notebook_command;
mod pull_command;
mod pull_images_command;
mod push_command;
mod rename_command;
mod repository_add_command;
mod repository_delete_command;
mod repository_list_command;
mod reset_command;
mod search_command;
mod set_watermark_command;
mod sql_server_command;
mod sql_shell_command;
mod system_api_server_gql_query_command;
mod system_api_server_gql_schema_command;
mod system_api_server_run_command;
mod system_debug_token_command;
mod system_decode_command;
mod system_diagnose_command;
mod system_e2e_command;
mod system_generate_token_command;
mod system_info_command;
mod system_ipfs_add_command;
mod tail_command;
mod ui_command;
mod upgrade_workspace_command;
mod verify_command;

pub use add_command::*;
pub use alias_add_command::*;
pub use alias_delete_command::*;
pub use alias_list_command::*;
pub use compact_command::*;
pub use complete_command::*;
pub use completions_command::*;
pub use config_command::*;
pub use delete_command::*;
pub use export_command::*;
pub use gc_command::*;
pub use ingest_command::*;
pub use init_command::*;
pub use inspect_lineage_command::*;
pub use inspect_query_command::*;
pub use inspect_schema_command::*;
pub use list_command::*;
pub use log_command::*;
pub use login_command::*;
pub use login_silent_command::*;
pub use logout_command::*;
pub use new_dataset_command::*;
pub use notebook_command::*;
pub use pull_command::*;
pub use pull_images_command::*;
pub use push_command::*;
pub use rename_command::*;
pub use repository_add_command::*;
pub use repository_delete_command::*;
pub use repository_list_command::*;
pub use reset_command::*;
pub use search_command::*;
pub use set_watermark_command::*;
pub use sql_server_command::*;
pub use sql_shell_command::*;
pub use system_api_server_gql_query_command::*;
pub use system_api_server_gql_schema_command::*;
pub use system_api_server_run_command::*;
pub use system_debug_token_command::*;
pub use system_decode_command::*;
pub use system_diagnose_command::*;
pub use system_e2e_command::*;
pub use system_generate_token_command::*;
pub use system_info_command::*;
pub use system_ipfs_add_command::*;
pub use tail_command::*;
pub use ui_command::*;
pub use upgrade_workspace_command::*;
pub use verify_command::*;

pub use super::error::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait(?Send)]
pub trait Command {
    /// Symbolic name of the command
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Whether the command requires an initialized workspace
    fn needs_workspace(&self) -> bool {
        true
    }

    /// Will be called before running to perform various argument sanity checks
    async fn validate_args(&self) -> Result<(), CLIError> {
        Ok(())
    }

    async fn run(&mut self) -> Result<(), CLIError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct NoOpCommand;

#[async_trait::async_trait(?Send)]
impl Command for NoOpCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
