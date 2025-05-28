// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use dill::TypedBuilder;
use kamu::ResourceLoaderImpl;
use kamu::domain::*;
use kamu_cli::CLIError;
use kamu_cli::commands::*;

#[test_log::test(tokio::test)]
async fn test_ambiguity_is_punished() {
    let cmd = NewDatasetCommand::builder(
        odf::DatasetName::from_str("foo").unwrap(),
        false,
        false,
        None,
    )
    .get(&dill::Catalog::builder().build())
    .unwrap();

    assert!(matches!(cmd.run().await, Err(CLIError::UsageError { .. })));
}

#[test_log::test(tokio::test)]
async fn test_root_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let cmd = NewDatasetCommand::builder(
        odf::DatasetName::from_str("foo").unwrap(),
        true,
        false,
        Some(path.clone()),
    )
    .get(&dill::Catalog::builder().build())
    .unwrap();

    cmd.run().await.unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .await
        .expect("Failed to parse template");
}

#[test_log::test(tokio::test)]
async fn test_derivative_dataset_parses() {
    let tempdir = tempfile::tempdir().unwrap();
    let path = tempdir.path().join("ds.yaml");
    let cmd = NewDatasetCommand::builder(
        odf::DatasetName::from_str("foo").unwrap(),
        false,
        true,
        Some(path.clone()),
    )
    .get(&dill::Catalog::builder().build())
    .unwrap();

    cmd.run().await.unwrap();

    let loader = ResourceLoaderImpl::new();
    loader
        .load_dataset_snapshot_from_path(&path)
        .await
        .expect("Failed to parse template");
}
