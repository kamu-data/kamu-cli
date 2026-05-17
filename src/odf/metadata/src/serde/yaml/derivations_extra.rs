// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for super::AccountRef {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            id: None,
            name: Some(s.into()),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for super::Secret {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            value: s.into(),
            content_encoding: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::str::FromStr for super::Variable {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self {
            value: s.into(),
            content_encoding: None,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
