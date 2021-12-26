// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::domain::*;
use kamu::infra::MetadataRepositoryNull;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::sync::Arc;

fn main() {
    let pull_svc = Arc::new(TestPullService {});
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(MetadataRepositoryNull),
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

impl PullService for TestPullService {
    fn pull_multi(
        &self,
        _dataset_refs: &mut dyn Iterator<Item = DatasetRefAny>,
        _options: PullOptions,
        ingest_listener: Option<Arc<dyn IngestMultiListener>>,
        _transform_listener: Option<Arc<dyn TransformMultiListener>>,
        _sync_listener: Option<Arc<dyn SyncMultiListener>>,
    ) -> Vec<(DatasetRefAny, Result<PullResult, PullError>)> {
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
        vec![(
            hdl.into(),
            Ok(PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks: 1,
            }),
        )]
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
