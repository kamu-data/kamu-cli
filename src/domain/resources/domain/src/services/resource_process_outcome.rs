// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::ResourceVersion;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What a `ResourceController` decided for one resource
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceProcessOutcome {
    /// Nothing more to do until the resource changes. Carries the version the
    /// decision was based on, after the controller's own writes: an older
    /// version makes the runtime ask once more.
    Done(ResourceVersion),

    /// Ask again at this moment: a transient failure, or a step that waits for
    /// time. A moment already passed means the next step is due at once.
    RetryAt(DateTime<Utc>),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
