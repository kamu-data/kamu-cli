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

pub(crate) fn json_to_yaml_value(value: &serde_json::Value) -> serde_yaml::Value {
    match value {
        serde_json::Value::Null => serde_yaml::Value::Null,
        serde_json::Value::Bool(value) => serde_yaml::Value::Bool(*value),
        serde_json::Value::Number(value) => {
            if let Some(value) = value.as_u64() {
                serde_yaml::to_value(value).unwrap()
            } else if let Some(value) = value.as_i64() {
                serde_yaml::to_value(value).unwrap()
            } else {
                serde_yaml::to_value(value.as_f64().unwrap()).unwrap()
            }
        }
        serde_json::Value::String(value) => serde_yaml::Value::String(value.clone()),
        serde_json::Value::Array(values) => {
            serde_yaml::Value::Sequence(values.iter().map(json_to_yaml_value).collect())
        }
        serde_json::Value::Object(entries) => serde_yaml::Value::Mapping(
            entries
                .iter()
                .map(|(key, value)| {
                    (
                        serde_yaml::Value::String(key.clone()),
                        json_to_yaml_value(value),
                    )
                })
                .collect(),
        ),
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
