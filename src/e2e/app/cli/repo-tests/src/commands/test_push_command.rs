// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_unknown_dataset_id_fails(kamu: KamuCliPuppet) {
    let dataset_id = odf::DatasetID::new_seeded_ed25519(b"unknown")
        .as_did_str()
        .to_string();
    let repo_url = Url::from_directory_path(kamu.workspace_path().join("repo"))
        .unwrap()
        .join("dataset")
        .unwrap();

    kamu.assert_failure_command_execution(
        ["push", &dataset_id, "--to", repo_url.as_str()],
        None,
        Some([format!("Error: Dataset not found: {dataset_id}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
