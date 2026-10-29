// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches;
use std::sync::Arc;

use container_runtime::{ContainerRuntime, ContainerRuntimeConfig};
use kamu::domain::*;
use kamu::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_unsupported_engine() {
    let harness = EngineProvisionerHarness::new();

    let res = harness.provision_engine("cobol").await;

    assert_matches!(
        res,
        Err(EngineProvisioningError::UnsupportedEngine(UnsupportedEngineError { engine_id }))
            if engine_id == "cobol"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_group::group(containerized, engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_engine_name_is_case_insensitive() {
    let harness = EngineProvisionerHarness::new();

    let res = harness.provision_engine("DataFusion").await;

    assert_matches!(res, Ok(()));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EngineProvisionerHarness {
    _tempdir: tempfile::TempDir,
    provisioner: EngineProvisionerLocal,
}

impl EngineProvisionerHarness {
    fn new() -> Self {
        let tempdir = tempfile::tempdir().unwrap();
        let run_info_dir = Arc::new(RunInfoDir::new(tempdir.path().join("run")));
        std::fs::create_dir(run_info_dir.as_path()).unwrap();

        Self {
            provisioner: EngineProvisionerLocal::new(
                EngineProvisionerLocalConfig::default(),
                Arc::new(ContainerRuntime::new(ContainerRuntimeConfig::default())),
                run_info_dir,
            ),
            _tempdir: tempdir,
        }
    }

    async fn provision_engine(&self, engine_id: &str) -> Result<(), EngineProvisioningError> {
        self.provisioner
            .provision_engine(engine_id, None)
            .await
            .map(|_| ())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
