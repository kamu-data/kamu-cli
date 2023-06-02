// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use async_graphql::*;

/////////////////////////////////////////////////////////////////////////////////////////
// TaskID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskID(String);

impl From<&str> for TaskID {
    fn from(value: &str) -> Self {
        Self(value.to_owned())
    }
}

impl From<String> for TaskID {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl Into<String> for TaskID {
    fn into(self) -> String {
        self.0
    }
}
impl Deref for TaskID {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for TaskID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            Ok(Self::from(value.as_str()))
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

/////////////////////////////////////////////////////////////////////////////////////////
// TaskStatus
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Finished,
}
