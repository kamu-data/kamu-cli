// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::distributions::{Alphanumeric, Uniform};
use rand::prelude::Distribution;

pub fn get_random_string(
    prefix_maybe: Option<&str>,
    random_length: usize,
    allowed_symbols: &AllowedSymbols,
) -> String {
    let prefix = prefix_maybe.unwrap_or("");
    let mut rng = rand::thread_rng();

    let random_part: String = (0..random_length)
        .map(|_| match allowed_symbols {
            AllowedSymbols::Alphanumeric => char::from(Alphanumeric.sample(&mut rng)),
            AllowedSymbols::AsciiSymbols => Uniform::from('!'..='~').sample(&mut rng),
        })
        .collect();

    format!("{prefix}{random_part}")
}

pub enum AllowedSymbols {
    Alphanumeric,
    AsciiSymbols,
}
