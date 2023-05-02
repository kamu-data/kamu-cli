// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::SourceState;

impl SourceState {
    pub const KIND_ETAG: &str = "odf/etag";
    pub const KIND_LAST_MODIFIED: &str = "odf/last-modified";
    pub const SOURCE_POLLING: &str = "odf/polling";
}
