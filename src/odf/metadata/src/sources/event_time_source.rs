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

crate::impl_parse_error!(EventTimeSource);

crate::impl_try_from_str!(EventTimeSource);

impl std::str::FromStr for EventTimeSource {
    type Err = ParseError<Self>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "FromMetadata" | "fromMetadata" | "frommetadata" => {
                Ok(Self::FromMetadata(EventTimeSourceFromMetadata {}))
            }
            "FromSystemTime" | "fromSystemTime" | "fromsystemtime" => {
                Ok(Self::FromSystemTime(EventTimeSourceFromSystemTime {}))
            }
            _ => Err(ParseError::new(s)),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
