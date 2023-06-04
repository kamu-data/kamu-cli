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
use kamu_domain_task_system as domain;

/////////////////////////////////////////////////////////////////////////////////////////
// TaskID
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaskID(domain::TaskID);

impl From<domain::TaskID> for TaskID {
    fn from(value: domain::TaskID) -> Self {
        TaskID(value)
    }
}

impl Into<domain::TaskID> for TaskID {
    fn into(self) -> domain::TaskID {
        self.0
    }
}

impl Deref for TaskID {
    type Target = domain::TaskID;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for TaskID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(s) = &value {
            match s.parse() {
                Ok(i) => Ok(Self(domain::TaskID::new(i))),
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

/////////////////////////////////////////////////////////////////////////////////////////
// TaskStatus
/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Queued,
    Running,
    Finished,
}
