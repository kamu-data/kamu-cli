// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{ResourceControllerName, TypeUri};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Which controller asks for actionable resources of which schema, and how it
/// is registered.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionableResourcesQuery {
    pub controller: ResourceControllerName,
    pub schema: TypeUri,
    /// Rows written by another revision no longer suppress a finished
    /// resource
    pub controller_revision: u32,
    /// Whether a status change by another writer makes a resource actionable
    pub reacts_to_status: bool,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
