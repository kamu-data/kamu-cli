#![feature(backtrace)]

use std::backtrace::BacktraceStatus;
use std::error::Error;

use console::style;
use kamu::infra::VolumeLayout;

fn main() {
    let workspace_layout = kamu_cli::find_workspace();
    let local_volume_layout = VolumeLayout::new(&workspace_layout.local_volume_dir);
    let matches = kamu_cli::cli().get_matches();

    let result = kamu_cli::run(workspace_layout, local_volume_layout, matches);

    match result {
        Ok(_) => (),
        Err(err) => {
            display_error(err);
            std::process::exit(1);
        }
    }
}

fn display_error(err: kamu_cli::CLIError) {
    eprintln!("{}: {}", style("Error").red().bold(), err);
    if let Some(bt) = err.backtrace() {
        if bt.status() == BacktraceStatus::Captured {
            eprintln!("\nBacktrace:\n{}", style(bt).dim().bold());
        }
    }
}
