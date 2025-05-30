// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::Parser as _;

fn main() {
    // TODO: Currently we are compiling `rustls` with both `ring` and `aws-cl-rs`
    // backends and since v0.23 `rustls` requires to disambiguate between which
    // one to use. Eventually we should unify all dependencies around the same
    // backend, but a number of them don't yet expose the necessary feature flags.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Could not install default TLS provider");

    let workspace_layout = kamu_cli::WorkspaceService::find_workspace();
    let args = kamu_cli::cli::Cli::parse();
    let propagate_default_panic = args.verbose == 0;
    observability::axum::set_hook_capture_panic_backtraces_no_propagate(propagate_default_panic);

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let result = runtime.block_on(kamu_cli::run(workspace_layout, args));

    match result {
        Ok(_) => (),
        Err(_) => {
            std::process::exit(1);
        }
    }
}
