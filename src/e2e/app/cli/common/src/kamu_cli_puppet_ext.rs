// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;

use crate::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuCliPuppetExt {
    async fn get_dataset_names(&self) -> Vec<String>;
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
            .skip(1)
            .map(|line| line.split(',').next().unwrap().to_string())
            .collect()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
