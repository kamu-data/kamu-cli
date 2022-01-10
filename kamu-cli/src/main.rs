// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![feature(backtrace)]

use std::backtrace::BacktraceStatus;
use std::error::Error;

use console::style;
use kamu::infra::VolumeLayout;
use kamu_cli::CLIError;

fn main() {
    let workspace_layout = kamu_cli::find_workspace();
    let local_volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let matches = kamu_cli::cli().get_matches();

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(kamu_cli::run(
        workspace_layout,
        local_volume_layout,
        matches,
    ));

    match result {
        Ok(_) => (),
        Err(err) => {
            display_error(err);
            std::process::exit(1);
        }
    }
}

fn display_error(err: CLIError) {
    match err {
        CLIError::CriticalFailure { .. } => {
            eprintln!("{}: {}", style("Critical Error").red().bold(), err);
            eprintln!(
                "Help us by reporting this problem at https://github.com/kamu-data/kamu-cli/issues"
            );
        }
        _ => {
            eprintln!("{}: {}", style("Error").red().bold(), err);
        }
    }

    if let Some(bt) = err.backtrace() {
        if bt.status() == BacktraceStatus::Captured {
            eprintln!("\nBacktrace:\n{}", style(bt).dim().bold());
        }
    }
}
