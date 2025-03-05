// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryFrom;

use crate::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

simple_string_scalar!(DatasetID, odf::DatasetID, from_did_str);
simple_string_scalar!(DatasetName, odf::DatasetName);
simple_string_scalar!(DatasetAlias, odf::DatasetAlias);
simple_string_scalar!(DatasetRefAny, odf::DatasetRefAny);
simple_string_scalar!(DatasetRef, odf::DatasetRef);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// DatasetRefRemote
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DatasetRefRemote(odf::DatasetRefRemote);

impl From<odf::DatasetRefRemote> for DatasetRefRemote {
    fn from(value: odf::DatasetRefRemote) -> Self {
        Self(value)
    }
}

impl std::fmt::Display for DatasetRefRemote {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[Scalar]
impl ScalarType for DatasetRefRemote {
    fn parse(value: Value) -> InputValueResult<Self> {
        if let Value::String(value) = &value {
            let val = odf::DatasetRefRemote::try_from(value.as_str())?;
            Ok(val.into())
        } else {
            Err(InputValueError::expected_type(value))
        }
    }

    fn to_value(&self) -> Value {
        Value::String(self.0.to_string())
    }
}
