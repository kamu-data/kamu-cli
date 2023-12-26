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
    pub const DEFAULT_SYSTEM_TIME_COLUMN_NAME: &'static str = "system_time";
    pub const DEFAULT_EVENT_TIME_COLUMN_NAME: &'static str = "event_time";
    pub const DEFAULT_OFFSET_COLUMN_NAME: &'static str = "offset";
}

impl MergeStrategySnapshot {
    pub const DEFAULT_OBSV_COLUMN_NAME: &'static str = "observed";
    pub const DEFAULT_OBSV_ADDED: &'static str = "I";
    pub const DEFAULT_OBSV_CHANGED: &'static str = "U";
    pub const DEFAULT_OBSV_REMOVED: &'static str = "D";
}

impl SourceState {
    pub const KIND_ETAG: &'static str = "odf/etag";
    pub const KIND_LAST_MODIFIED: &'static str = "odf/last-modified";
}
