// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use kamu_task_system as ts;

use crate::prelude::*;

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskID(ts::TaskID);

/////////////////////////////////////////////////////////////////////////////////////////

impl From<ts::TaskID> for TaskID {
    fn from(value: ts::TaskID) -> Self {
        TaskID(value)
    }
}

impl Into<ts::TaskID> for TaskID {
    fn into(self) -> ts::TaskID {
        self.0
    }
}

impl Deref for TaskID {
    type Target = ts::TaskID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for TaskID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            match s.parse() {
                Ok(i) => Ok(Self(ts::TaskID::new(i))),
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
