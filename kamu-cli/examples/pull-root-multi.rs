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
use kamu::infra::*;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use opendatafabric::*;

#[tokio::main]
async fn main() {
    let tempdir = tempfile::tempdir().unwrap();
    let pull_svc = Arc::new(TestPullService {});
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(DatasetRepositoryLocalFs::new(Arc::new(
            WorkspaceLayout::create(tempdir.path()).unwrap(),
        ))),
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

impl TestPullService {
    fn ingest(hdl: DatasetHandle, listener: Arc<dyn IngestListener>) -> PullResponse {
        let sleep = |t| std::thread::sleep(std::time::Duration::from_millis(t));
        let sleep_rand = |min: u64, max: u64| {
            let d = (max - min) as f32;
            let t = ((d * rand::random::<f32>()) as u64) + min;
            std::thread::sleep(std::time::Duration::from_millis(t));
        };

        listener.begin();

        sleep_rand(200, 1500);
        listener.on_stage_progress(IngestStage::CheckCache, 0, 0);

        sleep_rand(200, 1500);
        let size = 10u64.pow(6) + ((20 * 10u64.pow(6)) as f64 * rand::random::<f64>()) as u64;
        let download_time = 0.1 + 2.0 * rand::random::<f64>();
        let chunk_size = 1024usize;
        let num_chunks = size / chunk_size as u64;
        let chunk_sleep = (download_time / num_chunks as f64 * 1000f64) as u64;
        for i in (0..size).step_by(chunk_size) {
            listener.on_stage_progress(IngestStage::Fetch, i, size);
            sleep(chunk_sleep);
        }

        listener.on_stage_progress(IngestStage::Prepare, 0, 0);

        sleep_rand(200, 1500);
        listener.on_stage_progress(IngestStage::Read, 0, 0);

        sleep_rand(200, 1500);
        listener.on_stage_progress(IngestStage::Preprocess, 0, 0);

        sleep_rand(200, 1500);
        listener.on_stage_progress(IngestStage::Merge, 0, 0);

        sleep_rand(200, 1500);
        listener.on_stage_progress(IngestStage::Commit, 0, 0);

        sleep_rand(200, 1500);

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

        PullResponse {
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
        }
    }

    fn transform(hdl: DatasetHandle, listener: Arc<dyn TransformListener>) -> PullResponse {
        listener.begin();

        std::thread::sleep(std::time::Duration::from_millis(2000));

        let old_head = rand_hash();
        let new_head = rand_hash();
        listener.success(&TransformResult::Updated {
            new_head: new_head.clone(),
            old_head: old_head.clone(),
            num_blocks: 1,
        });

        PullResponse {
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
        }
    }
}

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
        let mut requests = dataset_refs.map(|r| match r.as_local_single_tenant_ref() {
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
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        _sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Result<Vec<PullResponse>, InternalError> {
        let in_l = ingest_listener.unwrap();
        let tr_l = transform_listener.unwrap();

        let ingest_handles: Vec<_> = [
            "org.geonames.cities",
            "com.naturalearthdata.admin0",
            "gov.census.data",
        ]
        .into_iter()
        .map(|s| {
            DatasetHandle::new(
                DatasetID::from_pub_key_ed25519(s.as_bytes()),
                s.try_into().unwrap(),
            )
        })
        .map(|hdl| {
            let listener = in_l.begin_ingest(&hdl).unwrap();
            std::thread::spawn(move || Self::ingest(hdl, listener))
        })
        .collect();

        let ingest_results: Vec<_> = ingest_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        let transform_handles: Vec<_> =
            ["com.acme.census.normalized", "com.acme.census.geolocated"]
                .into_iter()
                .map(|s| {
                    DatasetHandle::new(
                        DatasetID::from_pub_key_ed25519(s.as_bytes()),
                        s.try_into().unwrap(),
                    )
                })
                .map(|hdl| {
                    let listener = tr_l.begin_transform(&hdl).unwrap();
                    std::thread::spawn(move || Self::transform(hdl, listener))
                })
                .collect();

        let mut results = ingest_results;
        results.extend(transform_handles.into_iter().map(|h| h.join().unwrap()));
        Ok(results)
    }

    async fn set_watermark(
        &self,
        _dataset_ref: &DatasetRef,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, SetWatermarkError> {
        unimplemented!()
    }
}
