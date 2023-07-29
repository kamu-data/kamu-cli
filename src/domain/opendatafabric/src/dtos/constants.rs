// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::*;

impl DatasetVocabulary {
    pub const DEFAULT_SYSTEM_TIME_COLUMN_NAME: &str = "system_time";
    pub const DEFAULT_EVENT_TIME_COLUMN_NAME: &str = "event_time";
    pub const DEFAULT_OFFSET_COLUMN_NAME: &str = "offset";
}

impl MergeStrategySnapshot {
    pub const DEFAULT_OBSV_COLUMN_NAME: &str = "observed";
    pub const DEFAULT_OBSV_ADDED: &str = "I";
    pub const DEFAULT_OBSV_CHANGED: &str = "U";
    pub const DEFAULT_OBSV_REMOVED: &str = "D";
}

impl SourceState {
    pub const KIND_ETAG: &str = "odf/etag";
    pub const KIND_LAST_MODIFIED: &str = "odf/last-modified";
    pub const SOURCE_POLLING: &str = "odf/polling";
}
