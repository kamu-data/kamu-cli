// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use clap::ArgMatches;
use dill::component;

/////////////////////////////////////////////////////////////////////////////////////////

pub struct UserService {
    default_user_name: String,
}

#[component(pub)]
impl UserService {
    pub fn new() -> Self {
        Self {
            default_user_name: whoami::username(),
        }
    }

    pub fn user_indication(&self, arg_matches: &ArgMatches) -> UserIndication {
        let (current_user, current_user_specified_explicitly) =
            if let Some(user) = arg_matches.get_one::<String>("user") {
                (user, true)
            } else {
                (&self.default_user_name, false)
            };

        let target_user = if let Some(target_user) = arg_matches.get_one::<String>("target-user") {
            TargetUser::Specific {
                user_name: target_user.clone(),
            }
        } else if arg_matches.get_flag("all-users") {
            TargetUser::AllUsers
        } else {
            TargetUser::Current
        };

        UserIndication::new(
            current_user.clone(),
            current_user_specified_explicitly,
            target_user,
        )
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UserIndication {
    pub current_user: String,
    pub current_user_specified_explicitly: bool,
    pub target_user: TargetUser,
}

impl UserIndication {
    pub fn new(
        current_user: String,
        current_user_specified_explicitly: bool,
        target_user: TargetUser,
    ) -> Self {
        Self {
            current_user,
            current_user_specified_explicitly,
            target_user,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.current_user_specified_explicitly || self.target_user != TargetUser::Current
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TargetUser {
    Current,
    Specific { user_name: String },
    AllUsers,
}

/////////////////////////////////////////////////////////////////////////////////////////
