// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Who issues a request in an access test, relative to the owner of the
/// target resource
#[derive(Debug, Clone, Copy)]
pub enum Caller {
    Anonymous,
    Owner,
    OtherAccount,
    Admin,
}

impl Caller {
    pub const ALL: [Caller; 4] = [
        Caller::Anonymous,
        Caller::Owner,
        Caller::OtherAccount,
        Caller::Admin,
    ];
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq)]
pub enum Access {
    Allowed,
    Denied(String),
}

impl Access {
    pub fn denied(message: &str) -> Self {
        Self::Denied(message.to_string())
    }

    /// Allowed when the response has no errors, otherwise denied with all
    /// error messages joined
    pub fn from_response(response: async_graphql::Response) -> Self {
        if response.errors.is_empty() {
            Self::Allowed
        } else {
            let messages = response
                .errors
                .into_iter()
                .map(|e| e.message)
                .collect::<Vec<_>>();
            Self::Denied(messages.join("; "))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, PartialEq, Eq)]
pub struct AccessMatrix {
    pub anonymous: Access,
    pub owner: Access,
    pub other_account: Access,
    pub admin: Access,
}

impl AccessMatrix {
    /// Runs `access_as` for every caller in turn
    pub async fn collect(access_as: impl AsyncFn(Caller) -> Access) -> Self {
        Self {
            anonymous: access_as(Caller::Anonymous).await,
            owner: access_as(Caller::Owner).await,
            other_account: access_as(Caller::OtherAccount).await,
            admin: access_as(Caller::Admin).await,
        }
    }

    pub fn everyone() -> Self {
        Self {
            anonymous: Access::Allowed,
            owner: Access::Allowed,
            other_account: Access::Allowed,
            admin: Access::Allowed,
        }
    }

    pub fn nobody(error: &str) -> Self {
        Self {
            anonymous: Access::denied(error),
            owner: Access::denied(error),
            other_account: Access::denied(error),
            admin: Access::denied(error),
        }
    }

    pub fn owner_only(error: &str) -> Self {
        Self {
            anonymous: Access::denied(error),
            owner: Access::Allowed,
            other_account: Access::denied(error),
            admin: Access::denied(error),
        }
    }

    pub fn owner_and_admin(error: &str) -> Self {
        Self {
            anonymous: Access::denied(error),
            owner: Access::Allowed,
            other_account: Access::denied(error),
            admin: Access::Allowed,
        }
    }

    pub fn get(&self, caller: Caller) -> &Access {
        match caller {
            Caller::Anonymous => &self.anonymous,
            Caller::Owner => &self.owner,
            Caller::OtherAccount => &self.other_account,
            Caller::Admin => &self.admin,
        }
    }

    /// Callers that are allowed get denied with `error` instead
    pub fn with_allowed_denied(self, error: &str) -> Self {
        let deny = |access| match access {
            Access::Allowed => Access::denied(error),
            denied @ Access::Denied(_) => denied,
        };
        Self {
            anonymous: deny(self.anonymous),
            owner: deny(self.owner),
            other_account: deny(self.other_account),
            admin: deny(self.admin),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
