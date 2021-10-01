// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::ResourceLoaderImpl;
use kamu_cli::commands::*;
use kamu_cli::CLIError;

#[test_env_log::test]
fn test_ambiguity_is_punished() {
    let mut cmd = NewDatasetCommand::new("foo", false, false, None::<&str>);
    assert!(matches!(cmd.run(), Err(CLIError::UsageError { .. })));
}

#[test_env_log::test]
fn test_root_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let mut cmd = NewDatasetCommand::new("foo", true, false, Some(&path));
    cmd.run().unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .expect("Failed to parse template");
}

#[test_env_log::test]
fn test_derivative_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let mut cmd = NewDatasetCommand::new("foo", false, true, Some(&path));
    cmd.run().unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .expect("Failed to parse template");
}
