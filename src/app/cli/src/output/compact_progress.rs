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

use kamu::domain::compact_service::{CompactionListener, CompactionMultiListener, CompactionPhase};
use opendatafabric::DatasetHandle;

#[derive(Clone)]
pub struct CompactionMultiProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub finished: Arc<AtomicBool>,
}

impl CompactionMultiProgress {
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

impl CompactionMultiListener for CompactionMultiProgress {
    fn begin_compact(&self, dataset_handle: &DatasetHandle) -> Option<Arc<dyn CompactionListener>> {
        Some(Arc::new(CompactionProgress::new(
            dataset_handle,
            &self.multi_progress,
        )))
    }
}

pub struct CompactionProgress {
    dataset_handle: DatasetHandle,
    curr_progress: indicatif::ProgressBar,
}

///////////////////////////////////////////////////////////////////////////////

impl CompactionProgress {
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

impl CompactionListener for CompactionProgress {
    fn begin(&self) {
        self.curr_progress
            .set_message(self.spinner_message("Compacting dataset"));
    }

    fn success(&self) {
        self.curr_progress.finish_with_message(
            self.spinner_message(
                console::style("Dataset compacted succesfully".to_owned()).green(),
            ),
        );
    }

    fn begin_phase(&self, phase: CompactionPhase) {
        let message = match phase {
            CompactionPhase::GatherChainInfo => "Gathering chain information",
            CompactionPhase::MergeDataslices => "Merging dataslices",
            CompactionPhase::CommitNewBlocks => "Commiting new blocks",
            CompactionPhase::CleanOldFiles => "Cleaning old dataset files",
        };
        self.curr_progress.set_message(message);
    }

    fn end_phase(&self, _phase: CompactionPhase) {}
}
