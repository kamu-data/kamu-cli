// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use observability::metrics::MetricsProvider;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct S3Metrics {
    pub s3_api_call_count_successful_num_total: prometheus::IntCounterVec,
    pub s3_api_call_count_failed_num_total: prometheus::IntCounterVec,
    pub s3_api_request_time_s_hist: prometheus::HistogramVec,
}

#[component(pub)]
#[interface(dyn MetricsProvider)]
#[scope(Singleton)]
impl S3Metrics {
    pub fn new(application: &str) -> Self {
        use prometheus::*;

        Self {
            s3_api_call_count_successful_num_total: IntCounterVec::new(
                Opts::new(
                    format!("{application}_s3_api_call_count_successful_num_total"),
                    "Number of successful AWS SDK S3 calls by storage URL and SDK method",
                ),
                &["storage_url", "sdk_method"],
            )
            .unwrap(),
            s3_api_call_count_failed_num_total: IntCounterVec::new(
                Opts::new(
                    format!("{application}_s3_api_call_count_failed_num_total"),
                    "Number of failed AWS SDK S3 calls by storage URL and SDK method",
                ),
                &["storage_url", "sdk_method"],
            )
            .unwrap(),
            s3_api_request_time_s_hist: HistogramVec::new(
                HistogramOpts::new(
                    format!("{application}_s3_api_request_time_s_hist"),
                    "Histogram of AWS SDK S3 API request latencies (in seconds) by storage URL \
                     and SDK method",
                ),
                &["storage_url", "sdk_method"],
            )
            .unwrap(),
        }
    }
}

impl MetricsProvider for S3Metrics {
    fn register(&self, reg: &prometheus::Registry) -> prometheus::Result<()> {
        reg.register(Box::new(
            self.s3_api_call_count_successful_num_total.clone(),
        ))?;
        reg.register(Box::new(self.s3_api_call_count_failed_num_total.clone()))?;
        reg.register(Box::new(self.s3_api_request_time_s_hist.clone()))?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
