// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::infra::VolumeLayout;

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
            kamu_cli::error::display_cli_error(&err);
            std::process::exit(1);
        }
    }
}
