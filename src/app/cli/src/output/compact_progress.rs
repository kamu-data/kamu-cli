// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use kamu::domain::{
    CompactingListener,
    CompactingMultiListener,
    CompactingPhase,
    CompactingResult,
};
use opendatafabric::DatasetHandle;

#[derive(Clone)]
pub struct CompactingMultiProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl CompactingMultiProgress {
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
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn finish(&self) {
        self.finished.store(true, Ordering::SeqCst);
    }
}

impl CompactingMultiListener for CompactingMultiProgress {
    fn begin_compact(&self, dataset_handle: &DatasetHandle) -> Option<Arc<dyn CompactingListener>> {
        Some(Arc::new(CompactingProgress::new(
            dataset_handle,
            &self.multi_progress,
        )))
    }
}

pub struct CompactingProgress {
    dataset_handle: DatasetHandle,
    curr_progress: indicatif::ProgressBar,
}

///////////////////////////////////////////////////////////////////////////////

impl CompactingProgress {
    pub fn new(
        dataset_handle: &DatasetHandle,
        multi_progress: &Arc<indicatif::MultiProgress>,
    ) -> Self {
        Self {
            dataset_handle: dataset_handle.clone(),
            curr_progress: multi_progress.add(Self::new_spinner("Initializing")),
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

    fn spinner_message<T: std::fmt::Display>(&self, msg: T) -> String {
        format!(
            "{} {}",
            msg,
            console::style(self.dataset_handle.alias.clone()).dim(),
        )
    }
}

///////////////////////////////////////////////////////////////////////////////

impl CompactingListener for CompactingProgress {
    fn begin(&self) {
        self.curr_progress
            .set_message(self.spinner_message("Compacting dataset"));
    }

    fn success(&self, res: &CompactingResult) {
        match res {
            CompactingResult::NothingToDo => {
                self.curr_progress.finish_with_message(
                    self.spinner_message(console::style("Dataset was left intact").green()),
                );
            }
            CompactingResult::Success {
                old_head: _,
                new_head: _,
                old_num_blocks,
                new_num_blocks,
            } => {
                self.curr_progress.finish_with_message(
                    self.spinner_message(
                        console::style(format!(
                            "Dataset compacted successfully ({old_num_blocks} -> {new_num_blocks} \
                             blocks)"
                        ))
                        .green(),
                    ),
                );
            }
        }
    }

    fn begin_phase(&self, phase: CompactingPhase) {
        let message = match phase {
            CompactingPhase::GatherChainInfo => "Gathering chain information",
            CompactingPhase::MergeDataslices => "Merging dataslices",
            CompactingPhase::CommitNewBlocks => "Committing new blocks",
        };
        self.curr_progress.set_message(message);
    }

    fn end_phase(&self, _phase: CompactingPhase) {}
}
