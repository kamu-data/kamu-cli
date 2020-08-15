use kamu::domain::*;
use kamu_cli::commands::*;
use kamu_cli::OutputFormat;

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
        &OutputFormat::default(),
    );
    cmd.run().unwrap();
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
        for i in (0..200000).step_by(1000) {
            listener.on_stage_progress(IngestStage::Fetch, i, 200000);
            sleep(5);
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

        let hash = "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a";
        let result = IngestResult::Updated {
            block_hash: hash.to_owned(),
        };
        listener.success(&result);

        (
            id,
            Ok(PullResult::Updated {
                block_hash: hash.to_owned(),
            }),
        )
    }
}

impl PullService for TestPullService {
    fn pull_multi(
        &mut self,
        _dataset_ids_iter: &mut dyn Iterator<Item = &DatasetID>,
        _recursive: bool,
        _all: bool,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        _transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let in_l = ingest_listener.unwrap();
        let handles: Vec<_> = [
            "org.geonames.cities",
            "com.naturalearthdata.admin0",
            "ca.statcan.census",
        ]
        .iter()
        .map(|s| DatasetIDBuf::try_from(*s).unwrap())
        .map(|id| {
            let listener = in_l.lock().unwrap().begin_ingest(&id).unwrap();
            std::thread::spawn(move || Self::ingest(id, listener))
        })
        .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    }
}
