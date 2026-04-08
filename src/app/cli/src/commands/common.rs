// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use chrono_humanize::HumanTime;
use kamu::domain::PullImageListener;

use crate::OutputConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullImageProgress {
    output_config: Arc<OutputConfig>,
    image_purpose: &'static str,
    progress_bar: Mutex<Option<indicatif::ProgressBar>>,
}

impl PullImageProgress {
    pub fn new(output_config: Arc<OutputConfig>, image_purpose: &'static str) -> Self {
        Self {
            output_config,
            image_purpose,
            progress_bar: Mutex::new(None),
        }
    }
}

impl PullImageListener for PullImageProgress {
    fn begin(&self, image: &str) {
        if !self.output_config.is_tty
            || self.output_config.verbosity_level != 0
            || self.output_config.quiet
        {
            return;
        }

        let s = indicatif::ProgressBar::new_spinner();
        let style = indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        s.set_style(style);
        s.set_message(format!("Pulling {} image {}", self.image_purpose, image));
        s.enable_steady_tick(Duration::from_millis(100));
        self.progress_bar.lock().unwrap().replace(s);
    }

    fn success(&self) {
        self.progress_bar.lock().unwrap().take();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) fn humanize_relative_date(timestamp: DateTime<Utc>) -> String {
    format!("{}", HumanTime::from(timestamp - Utc::now()))
}

pub(crate) fn humanize_data_size(size: u64) -> String {
    if size == 0 {
        return "-".to_owned();
    }

    use humansize::{BINARY, format_size};
    format_size(size, BINARY)
}

pub(crate) fn humanize_quantity(num: u64) -> String {
    if num == 0 {
        return "-".to_owned();
    }

    use num_format::{Locale, ToFormattedString};
    num.to_formatted_string(&Locale::en)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
