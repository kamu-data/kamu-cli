// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::*;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    let tempdir = tempfile::tempdir().unwrap();
    let pull_svc = Arc::new(TestPullService {});
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(LocalDatasetRepositoryImpl::new(Arc::new(
            WorkspaceLayout::create(tempdir.path()).unwrap(),
        ))),
        Arc::new(RemoteAliasesRegistryNull),
        Arc::new(OutputConfig {
            is_tty: true,
            ..Default::default()
        }),
        ["a"],
        false,
        false,
        false,
        None as Option<&str>,
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

#[async_trait::async_trait(?Send)]
impl PullService for TestPullService {
    async fn pull_multi(
        &self,
        dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let mut requests = dataset_refs.map(|r| {
            if let Some(local_ref) = r.as_local_ref() {
                PullRequest {
                    local_ref: Some(local_ref),
                    remote_ref: None,
                    ingest_from: None,
                }
            } else {
                PullRequest {
                    local_ref: None,
                    remote_ref: Some(r.as_remote_ref().unwrap()),
                    ingest_from: None,
                }
            }
        });

        self.pull_multi_ext(
            &mut requests,
            options,
            ingest_listener,
            transform_listener,
            sync_listener,
        )
        .await
    }

    async fn pull_multi_ext(
        &self,
        _requests: &mut dyn Iterator<Item = PullRequest>,
        _options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        _transform_listener: Option<Arc<dyn TransformMultiListener>>,
        _sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let hdl = DatasetHandle::new(
            DatasetID::from_pub_key_ed25519(b"org.geonames.cities"),
            "org.geonames.cities".try_into().unwrap(),
        );

        let multi_listener = ingest_listener.unwrap();
        let listener = multi_listener.begin_ingest(&hdl).unwrap();

        let sleep = |t| std::thread::sleep(std::time::Duration::from_millis(t));

        listener.begin();

        sleep(500);
        listener.on_stage_progress(IngestStage::CheckCache, 0, 0);

        sleep(1000);
        for i in (0..200000).step_by(1000) {
            listener.on_stage_progress(IngestStage::Fetch, i, 200000);
            sleep(5);
        }

        listener.on_stage_progress(IngestStage::Prepare, 0, 0);

        sleep(1000);
        listener.on_stage_progress(IngestStage::Read, 0, 0);

        sleep(1000);
        listener.on_stage_progress(IngestStage::Preprocess, 0, 0);

        sleep(1000);
        listener.on_stage_progress(IngestStage::Merge, 0, 0);

        sleep(1000);
        listener.on_stage_progress(IngestStage::Commit, 0, 0);

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
        _dataset_ref: &DatasetRefLocal,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, SetWatermarkError> {
        unimplemented!()
    }
}
