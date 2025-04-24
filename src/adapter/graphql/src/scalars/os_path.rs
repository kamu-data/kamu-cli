// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;
use std::path::PathBuf;
use std::str::FromStr;

use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OSPath(PathBuf);

impl From<PathBuf> for OSPath {
    fn from(v: PathBuf) -> Self {
        Self(v)
    }
}

impl From<OSPath> for PathBuf {
    fn from(val: OSPath) -> Self {
        val.0
    }
}

impl Deref for OSPath {
    type Target = PathBuf;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for OSPath {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let v = PathBuf::from_str(value.as_str())?;
            Ok(v.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_str().unwrap().to_owned())
    }
}
