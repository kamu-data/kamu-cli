// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use random_strings::get_random_name;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub fn get_staging_name() -> String {
    let prefix = ".pending-";
    get_random_name(Some(prefix), 16)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
