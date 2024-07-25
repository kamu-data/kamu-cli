// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::net::Ipv4Addr;
use std::path::PathBuf;

use async_trait::async_trait;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::serde::yaml::YamlDatasetSnapshotSerializer;
use opendatafabric::serde::DatasetSnapshotSerializer;
use opendatafabric::{DatasetRef, DatasetSnapshot};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuCliPuppetExt {
    async fn get_dataset_names(&self) -> Vec<String>;

    async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot);

    async fn get_list_of_repo_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> (Vec<String>, Vec<String>);

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send;

    async fn start_api_server(self, e2e_data_file_path: PathBuf);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuCliPuppetExt for KamuCliPuppet {
    async fn get_dataset_names(&self) -> Vec<String> {
        let assert = self
            .execute(["list", "--output-format", "csv"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        stdout
            .lines()
            .skip(1) // Skip header
            .map(|line| line.split(',').next().unwrap().to_string())
            .collect()
    }

    async fn add_dataset(&self, dataset_snapshot: DatasetSnapshot) {
        let content = YamlDatasetSnapshotSerializer
            .write_manifest(&dataset_snapshot)
            .unwrap();

        let mut f = tempfile::NamedTempFile::new().unwrap();

        f.as_file().write_all(&content).unwrap();
        f.flush().unwrap();

        self.execute(["add".as_ref(), f.path().as_os_str()])
            .await
            .success();
    }

    async fn get_list_of_repo_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> (Vec<String>, Vec<String>) {
        let assert = self
            .execute([
                "repo",
                "alias",
                "list",
                dataset_ref.to_string().as_str(),
                "--output-format",
                "csv",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        stdout
            .lines()
            .skip(1) // Skip header
            .fold((vec![], vec![]), |mut acc, line| {
                // Skip name
                let mut line_it = line.split(',').skip(1);

                let alias_kind = line_it.next().unwrap();
                let alias = line_it.next().unwrap().to_string();

                match alias_kind {
                    "pull" => acc.0.push(alias),
                    "push" => acc.1.push(alias),
                    _ => panic!("Unexpected alias kind: {alias_kind}"),
                }

                acc
            })
    }

    async fn complete<T>(&self, input: T, current: usize) -> Vec<String>
    where
        T: Into<String> + Send,
    {
        let assert = self
            .execute([
                "complete",
                input.into().as_str(),
                current.to_string().as_str(),
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        stdout.lines().map(ToString::to_string).collect()
    }

    async fn start_api_server(self, e2e_data_file_path: PathBuf) {
        let host = Ipv4Addr::LOCALHOST.to_string();

        self.execute([
            "--e2e-output-data-path",
            e2e_data_file_path.to_str().unwrap(),
            "system",
            "api-server",
            "--address",
            host.as_str(),
        ])
        .await
        .success();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
