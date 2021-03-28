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

pub struct TestPullService;

impl PullService for TestPullService {
    fn pull_multi(
        &mut self,
        _dataset_ids_iter: &mut dyn Iterator<Item = &DatasetID>,
        _options: PullOptions,
        ingest_listener: Option<Arc<Mutex<dyn IngestMultiListener>>>,
        _transform_listener: Option<Arc<Mutex<dyn TransformMultiListener>>>,
    ) -> Vec<(DatasetIDBuf, Result<PullResult, PullError>)> {
        let id = DatasetIDBuf::try_from("org.geonames.cities").unwrap();

        let multi_listener = ingest_listener.unwrap();
        let single_listener = multi_listener.lock().unwrap().begin_ingest(&id).unwrap();
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

        let hash =
            Sha3_256::from_str("a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a")
                .unwrap();

        let result = IngestResult::Updated {
            block_hash: hash.to_owned(),
            has_more: false,
            uncacheable: false,
        };
        listener.success(&result);
        vec![(
            id,
            Ok(PullResult::Updated {
                block_hash: hash.to_owned(),
            }),
        )]
    }

    fn set_watermark(
        &mut self,
        _dataset_id: &DatasetID,
        _watermark: DateTime<Utc>,
    ) -> Result<PullResult, PullError> {
        unimplemented!()
    }
}
