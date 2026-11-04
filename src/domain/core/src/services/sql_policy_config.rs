// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::SqlPolicy;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Statements that SQL sessions accept besides queries, per kind of caller
#[derive(setty::Config, setty::Default)]
pub struct SqlPolicyConfig {
    /// Sessions of the local user's `kamu sql` shell, including `--command`
    /// and exports. Unset rules allow session views and session statements,
    /// and deny writes
    #[config(default)]
    pub local: SqlPolicyRulesConfig,

    /// Sessions of API callers: HTTP, GraphQL and `FlightSQL`. Unset rules
    /// deny everything but queries
    #[config(default)]
    pub api: SqlPolicyRulesConfig,
}

impl SqlPolicyConfig {
    /// The policy of the local user's own sessions
    pub fn local_policy(&self) -> SqlPolicy {
        self.local.resolve(SqlPolicy::LOCAL_INTERACTIVE)
    }

    /// The policy of sessions that serve API callers
    pub fn api_policy(&self) -> SqlPolicy {
        self.api.resolve(SqlPolicy::READ_ONLY)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Rules default per scope, so a config can set only some of them
#[derive(setty::Config, setty::Default)]
pub struct SqlPolicyRulesConfig {
    /// Allows `CREATE [TEMP] VIEW` and `DROP VIEW`. Views live only as long
    /// as the session and cannot replace a dataset
    pub allow_session_views: Option<bool>,

    /// Allows session statements such as `SET`, `PREPARE` and transactions
    pub allow_session_statements: Option<bool>,

    /// Allows statements that write or register data sources, such as
    /// `COPY ... TO`, `INSERT` and `CREATE EXTERNAL TABLE`
    pub allow_writes: Option<bool>,
}

impl SqlPolicyRulesConfig {
    fn resolve(&self, defaults: SqlPolicy) -> SqlPolicy {
        SqlPolicy {
            allow_session_views: self
                .allow_session_views
                .unwrap_or(defaults.allow_session_views),
            allow_session_statements: self
                .allow_session_statements
                .unwrap_or(defaults.allow_session_statements),
            allow_writes: self.allow_writes.unwrap_or(defaults.allow_writes),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
