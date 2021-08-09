use kamu::domain::*;
use kamu::infra::MetadataRepositoryNull;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::convert::TryFrom;
use std::sync::{Arc, Mutex};

fn main() {
    let pull_svc = Arc::new(TestPullService {});
    let mut cmd = PullCommand::new(
        pull_svc,
        Arc::new(MetadataRepositoryNull),
        Arc::new(OutputConfig::default()),
        ["a"].iter(),
        false,
        false,
        false,
        None,
        None,
    );
    cmd.run().unwrap();
}

fn rand_hash() -> Sha3_256 {
    use rand::RngCore;
    let mut hash = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut hash);
    Sha3_256::new(hash)
}

pub struct TestPullService;

impl PullService for TestPullService {
    fn pull_multi(
        &self,
        _dataset_ids_iter: &mut dyn Iterator<Item = &DatasetRef>,
        _options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        _transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
        _sync_listener: Option<Arc<Mutex<dyn SyncMultiListener>>>,
    ) -> Vec<(DatasetRefBuf, Result<PullResult, PullError>)> {
        let id = DatasetRefBuf::try_from("org.geonames.cities").unwrap();

        let multi_listener = ingest_listener.unwrap();
        let single_listener = multi_listener
            .lock()
            .unwrap()
            .begin_ingest(id.local_id())
            .unwrap();
        let mut listener = single_listener.lock().unwrap();

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
            old_head,
            new_head,
            num_blocks: 1,
            has_more: false,
            uncacheable: false,
        };
        listener.success(&result);
        vec![(
            id,
            Ok(PullResult::Updated {
                old_head: Some(old_head),
                new_head,
                num_blocks: 1,
            }),
        )]
    }

    fn sync_from(
        &self,
        _remote_ref: &DatasetRef,
        _local_id: &DatasetID,
        _options: PullOptions,
        _listener: Option<Arc<Mutex<dyn SyncListener>>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    fn ingest_from(
        &self,
        _dataset_id: &DatasetID,
        _fetch: FetchStep,
        _options: PullOptions,
        _listener: Option<Arc<Mutex<dyn IngestListener>>>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }

    fn set_watermark(
        &self,
        _dataset_id: &DatasetID,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }
}
