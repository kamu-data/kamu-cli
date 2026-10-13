// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ApplyMultiProgress {
    pub multi_progress: Arc<indicatif::MultiProgress>,
    pub total_progress: indicatif::ProgressBar,
    pub finished: Arc<AtomicBool>,
}

impl ApplyMultiProgress {
    pub fn new(total_items: usize) -> Self {
        let multi_progress = Arc::new(indicatif::MultiProgress::new());
        let total_progress = multi_progress.add(Self::new_total_progress_bar(total_items));

        Self {
            multi_progress,
            total_progress,
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

    pub fn println<I>(&self, msg: I)
    where
        I: AsRef<str>,
    {
        let _ = self.multi_progress.println(msg.as_ref());
    }

    pub fn increment_completed(&self) {
        self.total_progress.inc(1);
    }

    pub fn begin(&self, source: impl Into<String>) -> ApplyProgress {
        ApplyProgress::new(source, &self.multi_progress)
    }

    fn new_total_progress_bar(total_items: usize) -> indicatif::ProgressBar {
        let progress_bar = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_bar()
            .template("{spinner:.cyan} Applying manifests: [{wide_bar:.cyan/blue}] {pos}/{len}")
            .unwrap()
            .progress_chars("#>-");
        progress_bar.set_style(style);
        progress_bar.set_length(u64::try_from(total_items).unwrap());
        progress_bar.set_position(0);
        progress_bar.enable_steady_tick(Duration::from_millis(100));
        progress_bar
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ApplyProgress {
    source: String,
    progress_bar: indicatif::ProgressBar,
}

impl ApplyProgress {
    fn new(source: impl Into<String>, multi_progress: &Arc<indicatif::MultiProgress>) -> Self {
        let source = source.into();
        let progress_bar = multi_progress.add(Self::new_spinner());
        let progress = Self {
            source,
            progress_bar,
        };

        progress.begin();
        progress
    }

    fn new_spinner() -> indicatif::ProgressBar {
        let progress_bar = indicatif::ProgressBar::hidden();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        progress_bar.set_style(style);
        progress_bar.enable_steady_tick(Duration::from_millis(100));
        progress_bar
    }

    fn begin(&self) {
        self.progress_bar.set_message(format!(
            "Applying manifest {}",
            console::style(&self.source).dim()
        ));
    }

    pub fn finish_with_message(&self, message: impl Into<String>) {
        self.progress_bar.finish_with_message(message.into());
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
