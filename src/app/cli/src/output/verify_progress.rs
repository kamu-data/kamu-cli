// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use kamu::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct VerificationMultiProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl VerificationMultiProgress {
    pub fn new() -> Self {
        Self {
            multi_progress: Arc::new(indicatif::MultiProgress::new()),
            finished: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn draw(&self) {
        loop {
            if self.finished.load(Ordering::SeqCst) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }

    pub fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }
}

impl VerificationMultiListener for VerificationMultiProgress {
    fn begin_verify(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Option<Arc<dyn VerificationListener>> {
        Some(Arc::new(VerificationProgress::new(
            dataset_handle,
            self.multi_progress.clone(),
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct VerificationProgress {
    dataset_handle: odf::DatasetHandle,
    _multi_progress: Arc<indicatif::MultiProgress>,
    curr_progress: indicatif::ProgressBar,
    state: Mutex<VerificationState>,
}

struct VerificationState {
    block_hash: Option<odf::Multihash>,
    block_index: usize,
    num_blocks: usize,
    phase: VerificationPhase,
}

impl VerificationProgress {
    fn new(
        dataset_handle: &odf::DatasetHandle,
        multi_progress: Arc<indicatif::MultiProgress>,
    ) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            curr_progress: multi_progress.add(Self::new_spinner("Initializing")),
            _multi_progress: multi_progress,
            state: Mutex::new(VerificationState {
                block_hash: None,
                block_index: 0,
                num_blocks: 0,
                phase: VerificationPhase::DataIntegrity,
            }),
        }
    }

    fn new_spinner(msg: &str) -> indicatif::ProgressBar {
        let pb = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        pb.set_style(style);
        pb.set_message(msg.to_owned());
        pb.enable_steady_tick(Duration::from_millis(100));
        pb
    }

    fn spinner_message<T: std::fmt::Display>(
        &self,
        step: usize,
        out_of: usize,
        msg: T,
        block: Option<&odf::Multihash>,
    ) -> String {
        let step_str = if out_of != 0 {
            format!("[{step}/{out_of}]")
        } else {
            "[-/-]".to_string()
        };

        let dataset = if let Some(block) = block {
            format!(
                "({} @ {})",
                self.dataset_handle.alias,
                block.as_multibase().short()
            )
        } else {
            format!("({})", self.dataset_handle.alias)
        };

        format!(
            "{} {} {}",
            console::style(step_str).bold().dim(),
            msg,
            console::style(dataset).dim(),
        )
    }

    fn save_state(
        &self,
        block_hash: &odf::Multihash,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        let mut s = self.state.lock().unwrap();
        s.block_hash = Some(block_hash.clone());
        s.block_index = block_index;
        s.num_blocks = num_blocks;
        s.phase = phase;
    }
}

impl VerificationListener for VerificationProgress {
    fn begin(&self) {
        self.curr_progress
            .set_message(self.spinner_message(0, 0, "Verifying dataset", None));
    }

    fn success(&self, _result: &VerificationResult) {
        let s = self.state.lock().unwrap();
        self.curr_progress.finish_with_message(self.spinner_message(
            s.num_blocks,
            s.num_blocks,
            console::style("Dataset is valid".to_owned()).green(),
            None,
        ));
    }

    fn error(&self, error: &VerificationError) {
        let s = self.state.lock().unwrap();
        let msg = match error {
            VerificationError::DataDoesNotMatchMetadata(..) => {
                "Validation error (data doesn't match metadata)".to_string()
            }
            VerificationError::DataNotReproducible(..) => {
                "Validation error (data is not reproducible)".to_string()
            }
            _ => "Error during transformation".to_string(),
        };
        self.curr_progress.finish_with_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            console::style(msg).red(),
            None,
        ));
    }

    fn begin_phase(&self, phase: VerificationPhase) {
        let message = match phase {
            VerificationPhase::DataIntegrity => "Verifying data integrity",
            VerificationPhase::ReplayTransform => "Replaying transformations",
            VerificationPhase::MetadataIntegrity => "Verifying metadata integrity",
        };
        self.curr_progress.set_message(message);
    }

    fn end_phase(&self, _phase: VerificationPhase) {}

    fn begin_block(
        &self,
        block_hash: &odf::Multihash,
        block_index: usize,
        num_blocks: usize,
        phase: VerificationPhase,
    ) {
        self.save_state(block_hash, block_index, num_blocks, phase);
        match phase {
            VerificationPhase::MetadataIntegrity => unreachable!(),
            VerificationPhase::DataIntegrity => {
                self.curr_progress.set_message(self.spinner_message(
                    block_index + 1,
                    num_blocks,
                    "Verifying data integrity",
                    Some(block_hash),
                ));
            }
            VerificationPhase::ReplayTransform => {
                self.curr_progress.set_message(self.spinner_message(
                    block_index + 1,
                    num_blocks,
                    "Replaying transformation",
                    Some(block_hash),
                ));
            }
        }
    }

    fn end_block(
        &self,
        _block_hash: &odf::Multihash,
        _block_index: usize,
        _num_blocks: usize,
        _phase: VerificationPhase,
    ) {
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        Some(self)
    }
}

impl TransformListener for VerificationProgress {
    fn get_engine_provisioning_listener(
        self: Arc<Self>,
    ) -> Option<Arc<dyn EngineProvisioningListener>> {
        Some(self)
    }
}

impl EngineProvisioningListener for VerificationProgress {
    fn begin(&self, engine_id: &str) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            format!("Waiting for engine {engine_id}"),
            s.block_hash.as_ref(),
        ));
    }

    fn success(&self) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            "Replaying transformation",
            s.block_hash.as_ref(),
        ));
    }

    fn get_pull_image_listener(self: Arc<Self>) -> Option<Arc<dyn PullImageListener>> {
        Some(self)
    }
}

impl PullImageListener for VerificationProgress {
    fn begin(&self, image: &str) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            format!("Pulling engine image {image}"),
            s.block_hash.as_ref(),
        ));
    }

    fn success(&self) {
        let s = self.state.lock().unwrap();
        self.curr_progress.set_message(self.spinner_message(
            s.block_index + 1,
            s.num_blocks,
            "Replaying transformation",
            s.block_hash.as_ref(),
        ));
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
