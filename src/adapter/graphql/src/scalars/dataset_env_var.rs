// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use uuid::Uuid;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct DatasetEnvVarID(Uuid);

impl From<Uuid> for DatasetEnvVarID {
    fn from(value: Uuid) -> Self {
        DatasetEnvVarID(value)
    }
}

impl From<DatasetEnvVarID> for Uuid {
    fn from(val: DatasetEnvVarID) -> Self {
        val.0
    }
}

impl Deref for DatasetEnvVarID {
    type Target = Uuid;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for DatasetEnvVarID {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = Uuid::try_parse(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

impl std::fmt::Display for DatasetEnvVarID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
