// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use kamu::domain::*;
use kamu::*;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use odf_metadata::*;

#[tokio::main]
async fn main() {
    let tempdir = tempfile::tempdir().unwrap();
    let pull_svc = Arc::new(TestPullService {});
    let workspace_layout = WorkspaceLayout::create(tempdir.path(), false).unwrap();
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(DatasetRepositoryLocalFs::new(
            workspace_layout.datasets_dir.clone(),
            Arc::new(CurrentAccountSubject::new_test()),
            false,
        )),
        Arc::new(RemoteAliasesRegistryNull),
        Arc::new(OutputConfig {
            is_tty: true,
            ..Default::default()
        }),
        [DatasetRefAny::from_str("a").unwrap()],
        false,
        false,
        false,
        None as Option<DatasetName>,
        true,
        None as Option<&str>,
        false,
    );
    cmd.run().await.unwrap();
}

fn rand_hash() -> Multihash {
    use rand::RngCore;
    let mut hash = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut hash);
    Multihash::new(Multicodec::Sha3_256, &hash)
}

pub struct TestPullService;

#[async_trait::async_trait]
impl PullService for TestPullService {
    async fn pull(
        &self,
        _dataset_ref: &DatasetRefAny,
        _options: PullOptions,
        _listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    async fn pull_ext(
        &self,
        _request: &PullRequest,
        _options: PullOptions,
        _listener: Option<Arc<dyn PullListener>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    async fn pull_multi(
        &self,
        dataset_refs: Vec<DatasetRefAny>,
        options: PullMultiOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let requests = dataset_refs
            .into_iter()
            .map(|r| match r.as_local_single_tenant_ref() {
                Ok(local_ref) => PullRequest {
                    local_ref: Some(local_ref),
                    remote_ref: None,
                    ingest_from: None,
                },
                Err(remote_ref) => PullRequest {
                    local_ref: None,
                    remote_ref: Some(remote_ref),
                    ingest_from: None,
                },
            })
            .collect();

        self.pull_multi_ext(requests, options, listener).await
    }

    async fn pull_multi_ext(
        &self,
        _requests: Vec<PullRequest>,
        _options: PullMultiOptions,
        listener: Option<Arc<dyn PullMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let hdl = DatasetHandle::new(
            DatasetID::new_seeded_ed25519(b"org.geonames.cities"),
            "org.geonames.cities".try_into().unwrap(),
        );

        let multi_listener = listener.unwrap().get_ingest_listener().unwrap();
        let listener = multi_listener.begin_ingest(&hdl).unwrap();

        let sleep = |t| std::thread::sleep(std::time::Duration::from_millis(t));

        listener.begin();

        sleep(500);
        listener.on_stage_progress(IngestStage::CheckCache, 0, TotalSteps::Exact(0));

        sleep(1000);
        for i in (0..200000).step_by(1000) {
            listener.on_stage_progress(IngestStage::Fetch, i, TotalSteps::Exact(200000));
            sleep(5);
        }

        listener.on_stage_progress(IngestStage::Prepare, 0, TotalSteps::Exact(0));

        sleep(1000);
        listener.on_stage_progress(IngestStage::Read, 0, TotalSteps::Exact(0));

        sleep(1000);
        listener.on_stage_progress(IngestStage::Preprocess, 0, TotalSteps::Exact(0));

        sleep(1000);
        listener.on_stage_progress(IngestStage::Merge, 0, TotalSteps::Exact(0));

        sleep(1000);
        listener.on_stage_progress(IngestStage::Commit, 0, TotalSteps::Exact(0));

        sleep(1000);

        let old_head = rand_hash();
        let new_head = rand_hash();

        let result = IngestResult::Updated {
            old_head: old_head.clone(),
            new_head: new_head.clone(),
            num_blocks: 1,
            has_more: false,
            uncacheable: false,
        };
        listener.success(&result);
        Ok(vec![PullResponse {
            original_request: Some(PullRequest {
                local_ref: Some(hdl.as_local_ref()),
                remote_ref: None,
                ingest_from: None,
            }),
            local_ref: Some(hdl.as_local_ref()),
            remote_ref: None,
            result: Ok(PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks: 1,
            }),
        }])
    }

    async fn set_watermark(
        &self,
        _dataset_ref: &DatasetRef,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, SetWatermarkError> {
        unimplemented!()
    }
}
