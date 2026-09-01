// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod ready;
mod reconciling;

pub use ready::*;
pub use reconciling::*;

use crate::{ResourceConditionValue, ResourceExtensionValueError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_resource_condition_value(
    value: &serde_json::Value,
) -> Result<(), ResourceExtensionValueError> {
    serde_json::from_value::<ResourceConditionValue>(value.clone())
        .map(|_| ())
        .map_err(|err| {
            ResourceExtensionValueError::new(format!("condition value is invalid: {err}"))
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
