// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ValueRef {
    Literal(String),
    VariableRef(VariableValueRef),
    SecretRef(SecretValueRef),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl ValueRef {
    pub fn is_reference(&self) -> bool {
        !matches!(self, Self::Literal(_))
    }
}

impl fmt::Display for ValueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Literal(value) => f.write_str(value),
            Self::VariableRef(value) => write!(f, "var:{}/{}", value.variable_set, value.name),
            Self::SecretRef(value) => write!(f, "secret:{}/{}", value.secret_set, value.name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VariableValueRef {
    pub variable_set: String,
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SecretValueRef {
    pub secret_set: String,
    pub name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
