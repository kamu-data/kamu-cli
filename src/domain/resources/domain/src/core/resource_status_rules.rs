// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{DeclarativeResource, ResourceConditions, empty_resource_conditions};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The rules a resource type sets for its status. The defaults impose none
pub trait ResourceStatusRules: DeclarativeResource {
    /// Conditions every new resource of the type starts with. They are recorded
    /// in its `Created` event, so no status update follows creation
    fn initial_conditions(_spec: &Self::SpecInput) -> ResourceConditions {
        empty_resource_conditions()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
