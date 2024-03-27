// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::distributions::Alphanumeric;
use rand::Rng;

pub fn get_random_name(prefix_maybe: Option<&str>, random_length: usize) -> String {
    let prefix = prefix_maybe.unwrap_or("");
    let mut name = String::with_capacity(random_length + prefix.len());
    name.push_str(prefix);
    name.extend(
        rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(random_length)
            .map(char::from),
    );
    name
}
