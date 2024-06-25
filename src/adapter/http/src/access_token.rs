// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////

#[derive(Clone, Debug)]
pub struct AccessToken {
    pub token: String,
}

impl AccessToken {
    pub fn new<S>(token: S) -> Self
    where
        S: Into<String>,
    {
        Self {
            token: token.into(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////
