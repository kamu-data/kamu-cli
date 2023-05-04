// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::{CLIError, Command};
use kamu::domain::*;
use kamu::infra::WorkspaceLayout;

use std::sync::Arc;

pub struct GcCommand {
    workspace_layout: Arc<WorkspaceLayout>,
}

impl GcCommand {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }

    fn gc_cache(&self) -> Result<u64, std::io::Error> {
        let mut size_total = 0;
        for res in self.workspace_layout.cache_dir.read_dir()? {
            let entry = res?;
            let meta = entry.metadata()?;
            size_total += meta.len();
            std::fs::remove_file(entry.path())?;
        }
        Ok(size_total)
    }
}

#[async_trait::async_trait(?Send)]
impl Command for GcCommand {
    async fn run(&mut self) -> Result<(), CLIError> {
        let mut size_total = 0;
        // TODO: Extract GC code into respective cache/dataset repo services
        eprint!("Cleaning cache...");
        let size = self.gc_cache().int_err()?;
        size_total += size;
        if size != 0 {
            eprintln!(" ({})", humansize::format_size(size, humansize::BINARY));
        } else {
            eprintln!();
        }

        if size_total != 0 {
            eprintln!(
                "{} {} {}",
                console::style("Cleaned up").green().bold(),
                humansize::format_size(size_total, humansize::BINARY),
                console::style("in the workspace").green().bold(),
            );
        } else {
            eprintln!("{}", console::style("Workspace is already clean").yellow());
        }

        Ok(())
    }
}
