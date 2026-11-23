// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::{
    DeclarativeResource,
    ResourceConditions,
    ResourceStatus,
    ResourceStatusChangeRejectedError,
    empty_resource_conditions,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The rules a resource type sets for its status. The defaults impose none
pub trait ResourceStatusRules: DeclarativeResource {
    /// Conditions every new resource of the type starts with. They are recorded
    /// in its `Created` event, so no status update follows creation
    fn initial_conditions(_spec: &Self::SpecInput) -> ResourceConditions {
        empty_resource_conditions()
    }

    /// Decides whether a condition patch may change the status from `old` to
    /// `new`. Schemas check each condition's value; this checks the change,
    /// such as a transition one condition must not make or a combination of
    /// conditions
    fn validate_status_change(
        _old: &ResourceStatus,
        _new: &ResourceStatus,
    ) -> Result<(), ResourceStatusChangeRejectedError> {
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
