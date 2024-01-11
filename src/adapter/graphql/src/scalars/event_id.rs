// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use event_sourcing as evs;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventID(evs::EventID);

/////////////////////////////////////////////////////////////////////////////////////////

impl From<evs::EventID> for EventID {
    fn from(value: evs::EventID) -> Self {
        EventID(value)
    }
}

impl Into<evs::EventID> for EventID {
    fn into(self) -> evs::EventID {
        self.0
    }
}

impl Deref for EventID {
    type Target = evs::EventID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for EventID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            match s.parse() {
                Ok(i) => Ok(Self(evs::EventID::new(i))),
                Err(_) => Err(InputValueError::expected_type(value)),
            }
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
