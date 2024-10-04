// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::str::FromStr;

use kamu_auth_rebac as rebac;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// todo этот скаляр больше не нужен
#[derive(Debug)]
pub struct DatasetPropertyName(rebac::DatasetPropertyName);

impl From<rebac::DatasetPropertyName> for DatasetPropertyName {
    fn from(value: rebac::DatasetPropertyName) -> Self {
        DatasetPropertyName(value)
    }
}

impl From<DatasetPropertyName> for rebac::DatasetPropertyName {
    fn from(value: DatasetPropertyName) -> Self {
        value.0
    }
}

impl Deref for DatasetPropertyName {
    type Target = rebac::DatasetPropertyName;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// todo try scalar!(MyValue);

#[Scalar]
impl ScalarType for DatasetPropertyName {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = rebac::DatasetPropertyName::from_str(value)?;

            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
