// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use oso::PolarClass;

///////////////////////////////////////////////////////////////////////////////

#[derive(PolarClass, Debug, Clone)]
pub struct UserActor {
    #[polar(attribute)]
    pub name: String,
    #[polar(attribute)]
    pub anonymous: bool,
}

///////////////////////////////////////////////////////////////////////////////

impl UserActor {
    pub fn new(name: &str, anonymous: bool) -> Self {
        Self {
            name: name.to_string(),
            anonymous,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

impl std::fmt::Display for UserActor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "User(name='{}', anonymous={})",
            &self.name, self.anonymous
        )
    }
}

///////////////////////////////////////////////////////////////////////////////
