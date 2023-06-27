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
    pub default_user_name: String,
}

#[component(pub)]
impl UserService {
    pub fn new() -> Self {
        Self {
            default_user_name: whoami::username(),
        }
    }

    pub fn current_user_selection(&self, arg_matches: &ArgMatches) -> CurrentUserSelection {
        let (current_user, current_user_specified_explicitly) =
            if let Some(user) = arg_matches.get_one::<String>("user") {
                (user, true)
            } else {
                (&self.default_user_name, false)
            };

        CurrentUserSelection::new(current_user, current_user_specified_explicitly)
    }

    pub fn user_indication(&self, arg_matches: &ArgMatches) -> UserRelationIndication {
        let current_user = self.current_user_selection(arg_matches);

        let target_user = if let Some(target_user) = arg_matches.get_one::<String>("target-user") {
            TargetUserSeletion::Specific {
                user_name: target_user.clone(),
            }
        } else if arg_matches.get_flag("all-users") {
            TargetUserSeletion::AllUsers
        } else {
            TargetUserSeletion::Current
        };

        UserRelationIndication::new(current_user, target_user)
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct CurrentUserSelection {
    pub user_name: String,
    pub specified_explicitly: bool,
}

impl CurrentUserSelection {
    pub fn new<S>(user_name: S, specified_explicitly: bool) -> Self
    where
        S: Into<String>,
    {
        Self {
            user_name: user_name.into(),
            specified_explicitly,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.specified_explicitly
    }
}

/////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct UserRelationIndication {
    pub current_user: CurrentUserSelection,
    pub target_user: TargetUserSeletion,
}

impl UserRelationIndication {
    pub fn new(current_user: CurrentUserSelection, target_user: TargetUserSeletion) -> Self {
        Self {
            current_user,
            target_user,
        }
    }

    pub fn is_explicit(&self) -> bool {
        self.current_user.is_explicit() || self.target_user != TargetUserSeletion::Current
    }
}

#[derive(Debug, Eq, PartialEq)]
pub enum TargetUserSeletion {
    Current,
    Specific { user_name: String },
    AllUsers,
}

/////////////////////////////////////////////////////////////////////////////////////////
