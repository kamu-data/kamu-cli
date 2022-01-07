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

fn main() {
    let pull_svc = Arc::new(TestPullService {});
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(DatasetRegistryNull),
        Arc::new(RemoteAliasesRegistryNull),
        Arc::new(OutputConfig::default()),
        ["a"],
        false,
        false,
        false,
        None as Option<&str>,
        None as Option<&str>,
    );
    cmd.run().unwrap();
}

fn rand_hash() -> Multihash {
    use rand::RngCore;
    let mut hash = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut hash);
    Multihash::new(Multicodec::Sha3_256, &hash)
}

pub struct TestPullService;

impl TestPullService {
    fn ingest(
        hdl: DatasetHandle,
        listener: Arc<dyn IngestListener>,
    ) -> (DatasetRefAny, Result<PullResult, PullError>) {
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

        (
            hdl.into(),
            Ok(PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks: 1,
            }),
        )
    }

    fn transform(
        hdl: DatasetHandle,
        listener: Arc<dyn TransformListener>,
    ) -> (DatasetRefAny, Result<PullResult, PullError>) {
        listener.begin();

        std::thread::sleep(std::time::Duration::from_millis(2000));

        let old_head = rand_hash();
        let new_head = rand_hash();
        listener.success(&TransformResult::Updated {
            new_head: new_head.clone(),
            old_head: old_head.clone(),
            num_blocks: 1,
        });

        (
            hdl.into(),
            Ok(PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks: 1,
            }),
        )
    }
}

impl PullService for TestPullService {
    fn pull_multi(
        &self,
        _dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        _options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        transform_listener: Option<Arc<dyn TransformMultiListener>>,
        _sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(DatasetRefAny, Result<PullResult, PullError>)> {
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
        results
    }

    fn sync_from(
        &self,
        _remote_ref: &DatasetRefRemote,
        _local_name: &DatasetName,
        _options: PullOptions,
        _listener: Option<Arc<dyn SyncListener>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    fn ingest_from(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _fetch: FetchStep,
        _options: PullOptions,
        _listener: Option<Arc<dyn IngestListener>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    fn set_watermark(
        &self,
        _dataset_ref: &DatasetRefLocal,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }
}
