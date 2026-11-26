// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, TimeDelta, Utc};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// How long a deleted resource is kept before it may be purged. Retention
/// starts when the deletion completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceRetentionPolicy {
    PurgeImmediately,
    RetainFor(TimeDelta),
    RetainForever,
}

impl ResourceRetentionPolicy {
    /// The node's default policy when its configuration sets none
    pub const FRAMEWORK_DEFAULT: Self = Self::RetainFor(TimeDelta::days(7));

    /// When a resource deleted at `deleted_at` may be purged; `None` for never,
    /// including a retention reaching past the representable time
    pub fn purge_at(&self, deleted_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Self::PurgeImmediately => Some(deleted_at),
            Self::RetainFor(retention) => deleted_at.checked_add_signed(*retention),
            Self::RetainForever => None,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
