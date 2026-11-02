// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub trait ResourceValidateSpec {
    type ValidationError;

    fn validate(&self) -> Result<(), Self::ValidationError>;

    /// Validates a spec that replaces `current`. A resource kind overrides it
    /// to let entries that `current` already holds keep passing after its
    /// rules got stricter, so an old entry does not block every later update.
    fn validate_update(&self, _current: &Self) -> Result<(), Self::ValidationError> {
        self.validate()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
