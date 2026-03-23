// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValueRef {
    Literal(String),
    VariableRef { variable_set: String, name: String },
    SecretRef { secret_set: String, name: String },
}

impl ValueRef {
    pub fn is_reference(&self) -> bool {
        !matches!(self, Self::Literal(_))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
