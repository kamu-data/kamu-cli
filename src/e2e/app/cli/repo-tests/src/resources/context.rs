// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Abstraction over *where* resource CLI commands run, so a single scenario
/// body can be exercised against both the implicit `local` context and a
/// `remote` context backed by a running API server.
///
/// The enum owns the workspace [`KamuCliPuppet`] and, for the remote case, the
/// name of a registered context. Command helpers inject the right `--context`
/// flag and otherwise delegate to the puppet's assertion API, so scenario
/// bodies stay free of context plumbing.
pub enum ResourceCtx {
    /// Commands run against the puppet's own workspace (implicit `local`
    /// context).
    Local(KamuCliPuppet),
    /// Commands run against a registered remote context (`--context <name>`).
    Remote {
        kamu: KamuCliPuppet,
        context_name: String,
    },
}

impl ResourceCtx {
    /// Default remote context name used by [`ResourceCtx::remote_from_server`].
    pub const DEFAULT_REMOTE_CONTEXT: &'static str = "prod";

    /// Build a remote context from a running API server, encapsulating the
    /// login dance: obtain an e2e token, create a fresh multi-tenant CLI
    /// workspace, authenticate it against the server, and register the server
    /// as a named resource context.
    ///
    /// Mirrors the combined CLI↔server pattern in
    /// `crate::test_smart_transfer_protocol`.
    pub async fn remote_from_server(client: &mut KamuApiServerClient, context_name: &str) -> Self {
        let token = client.auth().login_as_e2e_user().await;
        let server_url = client.get_base_url().clone();

        let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

        // Store the odf-server token for this backend so the context can reuse it.
        kamu.execute([
            "login",
            server_url.as_str(),
            "--access-token",
            token.as_str(),
        ])
        .await
        .success();

        kamu.execute(["context", "add", context_name, "--url", server_url.as_str()])
            .await
            .success();

        Self::Remote {
            kamu,
            context_name: context_name.to_string(),
        }
    }

    /// The underlying CLI puppet (e.g. for `--account` switching or workspace
    /// inspection).
    pub fn kamu(&self) -> &KamuCliPuppet {
        match self {
            Self::Local(kamu) | Self::Remote { kamu, .. } => kamu,
        }
    }

    /// `--context <name>` flag for the remote case; empty for local.
    fn context_args(&self) -> Vec<String> {
        match self {
            Self::Local(_) => Vec::new(),
            Self::Remote { context_name, .. } => {
                vec!["--context".to_string(), context_name.clone()]
            }
        }
    }

    /// Build a full argument vector: `<args...>` followed by the context flag.
    /// Context args go last so callers can pass the subcommand + selectors as
    /// `args`.
    pub fn args<I, S>(&self, args: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut full: Vec<String> = args.into_iter().map(Into::into).collect();
        full.extend(self.context_args());
        full
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Thin command pass-throughs (context-flag-aware)
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /// Run an arbitrary resource command (with the context flag appended) and
    /// assert success. `expected` regex lines are matched against stderr (the
    /// CLI emits its human-readable status there; stdout carries data output).
    pub async fn assert_success<I, S>(&self, args: I, expected: Option<&[&str]>)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        self.kamu()
            .assert_success_command_execution(full, None, expected.map(<[&str]>::to_vec))
            .await;
    }

    /// Run an arbitrary resource command (with the context flag appended) and
    /// assert failure. `expected` regex lines are matched against stderr.
    pub async fn assert_failure<I, S>(&self, args: I, expected: Option<&[&str]>)
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        self.kamu()
            .assert_failure_command_execution(full, None, expected.map(<[&str]>::to_vec))
            .await;
    }

    /// Apply a manifest passed via stdin, asserting success.
    pub async fn apply_stdin(&self, manifest: &str, extra_args: &[&str]) {
        let mut args = vec!["apply".to_string(), "--stdin".to_string()];
        args.extend(extra_args.iter().map(ToString::to_string));
        args.extend(self.context_args());

        self.kamu()
            .execute_with_input(args, manifest.to_string())
            .await
            .success();
    }

    /// Run a resource command (context flag appended) and return raw stdout.
    pub async fn stdout<I, S>(&self, args: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        let result = self.kamu().execute(full).await.success();

        std::str::from_utf8(&result.get_output().stdout)
            .unwrap()
            .to_string()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
