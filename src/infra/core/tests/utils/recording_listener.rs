// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::{Arc, Mutex};

use kamu::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Records listener calls in order: `begin`, `success`, `error` and
/// `no_polling_source`
#[derive(Default)]
pub struct RecordingListener {
    events: Mutex<Vec<&'static str>>,
}

impl RecordingListener {
    pub fn events(&self) -> Vec<&'static str> {
        self.events.lock().unwrap().clone()
    }

    fn record(&self, event: &'static str) {
        self.events.lock().unwrap().push(event);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl SyncListener for RecordingListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &SyncResult) {
        self.record("success");
    }

    fn error(&self, _error: &SyncError) {
        self.record("error");
    }
}

impl CompactionListener for RecordingListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &CompactionResult) {
        self.record("success");
    }

    fn error(&self, _error: &CompactionError) {
        self.record("error");
    }
}

impl PushIngestListener for RecordingListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &PushIngestResult) {
        self.record("success");
    }

    fn error(&self, _error: &PushIngestDataError) {
        self.record("error");
    }
}

impl PullListener for RecordingListener {
    fn get_ingest_listener(self: Arc<Self>) -> Option<Arc<dyn PollingIngestListener>> {
        Some(self)
    }

    fn get_transform_listener(self: Arc<Self>) -> Option<Arc<dyn TransformListener>> {
        Some(self)
    }

    fn get_sync_listener(self: Arc<Self>) -> Option<Arc<dyn SyncListener>> {
        Some(self)
    }
}

impl TransformListener for RecordingListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &TransformResult) {
        self.record("success");
    }

    fn error(&self, _error: &PullError) {
        self.record("error");
    }
}

impl PollingIngestListener for RecordingListener {
    fn begin(&self) {
        self.record("begin");
    }

    fn success(&self, _result: &PollingIngestResult) {
        self.record("success");
    }

    fn error(&self, _error: &PullError) {
        self.record("error");
    }

    fn no_polling_source(&self) {
        self.record("no_polling_source");
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
