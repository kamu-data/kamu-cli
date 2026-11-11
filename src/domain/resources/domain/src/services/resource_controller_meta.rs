// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::ResourceControllerName;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Registration of a `ResourceController`. Each schema has one controller, and
/// each controller a name of its own.
#[derive(Debug, Clone)]
pub struct ResourceControllerMeta {
    /// Schema URI of the resource type the controller drives
    pub schema: &'static str,

    /// Name its bookkeeping is stored under
    pub controller_name: ResourceControllerName,

    /// Bumped by hand when the controller's semantics change, so that the
    /// resources it finished with are asked about once more
    pub controller_revision: u32,

    /// Whether a status change written by anyone makes a resource actionable
    pub reacts_to_status: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
