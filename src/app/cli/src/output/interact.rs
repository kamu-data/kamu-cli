// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::CLIError;

#[derive(Debug, Clone)]
pub struct Interact {
    /// Don't ask user for confirmation and assume 'yes'
    pub assume_yes: bool,
}

impl Interact {
    pub fn new(assume_yes: bool) -> Self {
        Self { assume_yes }
    }

    pub fn require_confirmation(&self, prompt: impl std::fmt::Display) -> Result<(), CLIError> {
        use read_input::prelude::*;

        let prompt = format!("{prompt}\nDo you wish to continue? [y/N]: ");

        let answer: String = input()
            .repeat_msg(prompt)
            .default("n".to_owned())
            .add_test(|v| matches!(v.as_ref(), "n" | "N" | "no" | "y" | "Y" | "yes"))
            .get();

        if !matches!(answer.as_ref(), "n" | "N" | "no") {
            Ok(())
        } else {
            Err(CLIError::Aborted)
        }
    }
}
