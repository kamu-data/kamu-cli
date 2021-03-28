use kamu::domain::*;
use kamu_cli::commands::*;
use kamu_cli::output::OutputConfig;
use opendatafabric::*;

use chrono::{DateTime, Utc};
use std::cell::RefCell;
use std::convert::TryFrom;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

fn main() {
    let pull_svc = Rc::new(RefCell::new(TestPullService {}));
    let mut cmd = PullCommand::new(
        pull_svc,
        ["a"].iter(),
        false,
        false,
        false,
        &OutputConfig::default(),
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

impl TestPullService {
    fn ingest(
        id: DatasetIDBuf,
        l: Arc<Mutex<dyn IngestListener>>,
    ) -> (DatasetIDBuf, Result<PullResult, PullError>) {
        let sleep = |t| std::thread::sleep(std::time::Duration::from_millis(t));
        let sleep_rand = |min: u64, max: u64| {
            let d = (max - min) as f32;
            let t = ((d * rand::random::<f32>()) as u64) + min;
            std::thread::sleep(std::time::Duration::from_millis(t));
        };

        let mut listener = l.lock().unwrap();

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

        let hash = rand_hash();
        let result = IngestResult::Updated {
            block_hash: hash.to_owned(),
            has_more: false,
            uncacheable: false,
        };
        listener.success(&result);

        (
            id,
            Ok(PullResult::Updated {
                block_hash: hash.to_owned(),
            }),
        )
    }

    fn transform(
        id: DatasetIDBuf,
        l: Arc<Mutex<dyn TransformListener>>,
    ) -> (DatasetIDBuf, Result<PullResult, PullError>) {
        let mut listener = l.lock().unwrap();

        listener.begin();

        std::thread::sleep(std::time::Duration::from_millis(2000));

        let new_hash = rand_hash();
        listener.success(&TransformResult::Updated {
            block_hash: new_hash.clone(),
        });

        (
            id,
            Ok(PullResult::Updated {
                block_hash: new_hash,
            }),
        )
    }
}

impl PullService for TestPullService {
    fn pull_multi(
        &mut self,
        _dataset_ids_iter: &mut dyn Iterator<Item = &DatasetID>,
        _options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let in_l = ingest_listener.unwrap();
        let tr_l = transform_listener.unwrap();

        let ingest_handles: Vec<_> = [
            "org.geonames.cities",
            "com.naturalearthdata.admin0",
            "gov.census.data",
        ]
        .iter()
        .map(|s| DatasetIDBuf::try_from(*s).unwrap())
        .map(|id| {
            let listener = in_l.lock().unwrap().begin_ingest(&id).unwrap();
            std::thread::spawn(move || Self::ingest(id, listener))
        })
        .collect();

        let ingest_results: Vec<_> = ingest_handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect();

        let transform_handles: Vec<_> =
            ["com.acme.census.normalized", "com.acme.census.geolocated"]
                .iter()
                .map(|s| DatasetIDBuf::try_from(*s).unwrap())
                .map(|id| {
                    let listener = tr_l.lock().unwrap().begin_transform(&id).unwrap();
                    std::thread::spawn(move || Self::transform(id, listener))
                })
                .collect();

        let mut results = ingest_results;
        results.extend(transform_handles.into_iter().map(|h| h.join().unwrap()));
        results
    }

    fn set_watermark(
        &mut self,
        _dataset_id: &DatasetID,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }
}
