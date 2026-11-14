// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use multiformats::ParseError;

use crate::sources::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

crate::impl_parse_error!(SourceCaching);

crate::impl_try_from_str!(SourceCaching);

impl std::str::FromStr for SourceCaching {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Forever" | "forever" => Ok(Self::Forever(SourceCachingForever {})),
            _ => Err(ParseError::new(s)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
