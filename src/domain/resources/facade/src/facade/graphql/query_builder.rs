// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(super) fn parse_enum<T>(value: &str, field_name: &str) -> Result<T, InternalError>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| {
        InternalError::new(format!(
            "Unsupported {field_name} '{value}' in remote resource list",
        ))
    })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
