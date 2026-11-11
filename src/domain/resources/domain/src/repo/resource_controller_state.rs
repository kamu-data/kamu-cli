// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};

use crate::{ResourceControllerName, ResourceID};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// What one controller last did with one resource, as the controller runtime
/// records it.
///
/// `not_before` tells the outcomes apart:
/// - unset: the controller finished with the resource at `handled_generation` and
///   `handled_status_generation`;
/// - set: a retry or a failure, and the resource waits until then.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResourceControllerState {
    pub controller: ResourceControllerName,
    pub resource_id: ResourceID,
    /// Revision of the controller's logic that wrote the row
    pub controller_revision: u32,
    pub handled_generation: Option<u64>,
    pub handled_status_generation: Option<u64>,
    pub not_before: Option<DateTime<Utc>>,
    /// Consecutive failed attempts, the base of the retry backoff
    pub attempts: u32,
    /// Kept for diagnostics only
    pub last_error: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
