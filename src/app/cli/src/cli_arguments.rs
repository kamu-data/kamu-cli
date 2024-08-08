// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ops::Deref;

use clap::Arg;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub mod add {
    use super::CliArgument;

    pub const PUBLIC: CliArgument = CliArgument::new("public");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CliArgument(&'static str);

impl CliArgument {
    pub const fn new(id: &'static str) -> Self {
        Self(id)
    }

    pub fn to_clap_arg_long(self) -> Arg {
        let id = self.0;

        Arg::new(id).long(id)
    }
}

impl Deref for CliArgument {
    type Target = &'static str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
