// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use super::{CLIError, Command};
use crate::GcService;

#[dill::component]
#[dill::interface(dyn Command)]
pub struct GcCommand {
    gc_service: Arc<GcService>,
}

#[async_trait::async_trait(?Send)]
impl Command for GcCommand {
    async fn run(&self) -> Result<(), CLIError> {
        eprint!("Cleaning cache...");
        let result = self.gc_service.purge_cache()?;
        if result.bytes_freed != 0 {
            eprintln!(
                " ({})",
                humansize::format_size(result.bytes_freed, humansize::BINARY)
            );
        } else {
            eprintln!();
        }

        if result.bytes_freed != 0 {
            eprintln!(
                "{} {} {}",
                console::style("Cleaned up").green().bold(),
                humansize::format_size(result.bytes_freed, humansize::BINARY),
                console::style("in the workspace").green().bold(),
            );
        } else {
            eprintln!("{}", console::style("Workspace is already clean").yellow());
        }

        Ok(())
    }
}
