// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use opendatafabric as odf;

use crate::prelude::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Multihash(odf::Multihash);

impl From<odf::Multihash> for Multihash {
    fn from(value: odf::Multihash) -> Self {
        Multihash(value)
    }
}

impl Into<odf::Multihash> for Multihash {
    fn into(self) -> odf::Multihash {
        self.0
    }
}

impl Deref for Multihash {
    type Target = odf::Multihash;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[Scalar]
impl ScalarType for Multihash {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let sha = odf::Multihash::from_multibase(value.as_str())?;
            Ok(sha.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
