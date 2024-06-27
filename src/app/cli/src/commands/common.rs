// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Mutex;
use std::time::Duration;

use kamu::domain::PullImageListener;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn prompt_yes_no(msg: &str) -> bool {
    use read_input::prelude::*;

    let answer: String = input()
        .repeat_msg(msg)
        .default("n".to_owned())
        .add_test(|v| matches!(v.as_ref(), "n" | "N" | "no" | "y" | "Y" | "yes"))
        .get();

    !matches!(answer.as_ref(), "n" | "N" | "no")
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct PullImageProgress {
    image_purpose: &'static str,
    progress_bar: Mutex<Option<indicatif::ProgressBar>>,
}

impl PullImageProgress {
    pub fn new(image_purpose: &'static str) -> Self {
        Self {
            image_purpose,
            progress_bar: Mutex::new(None),
        }
    }
}

impl PullImageListener for PullImageProgress {
    fn begin(&self, image: &str) {
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
