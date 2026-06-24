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

use super::fixtures;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Abstraction over *where* resource CLI commands run, so a single scenario
/// body can be exercised against both the implicit `local` context and a
/// `remote` context backed by a running API server.
///
/// The enum owns the workspace [`KamuCliPuppet`]. There are two ways to target
/// a remote context, and both are valid CLI usage:
///
/// 1. **Active-context switch** (this type's default): `context use <name>` is
///    run once at construction so plain commands target the remote. Used by the
///    general lifecycle scenarios.
/// 2. **Per-command `--context <name>`**: every resource command (`apply`,
///    `get`, `list`, `delete`, `summary`, `context api-resources`) flattens
///    `ResourceContextArgs` and accepts `--context`. Append it explicitly via
///    [`ResourceCtx::context_override_arg`]. The dedicated context-override
///    scenario focuses on this path.
///
/// The only command that does *not* accept `--context` is the bare `context`
/// switcher itself, so the flag must never be appended to it.
pub enum ResourceCtx {
    /// Commands run against the puppet's own workspace (implicit `local`
    /// context).
    Local(KamuCliPuppet),
    /// Commands run against a registered remote context that has been made the
    /// active context.
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
    /// workspace, authenticate it against the server, register the server as a
    /// named resource context, and switch the active context to it.
    ///
    /// Mirrors the combined CLI↔server pattern in
    /// `crate::test_smart_transfer_protocol`.
    ///
    /// Note on server-side config: the local CLI workspace created here is
    /// deliberately *unconfigured*. When a command targets a remote context,
    /// the CLI is a thin client and the work (e.g. `SecretSet` encryption)
    /// happens on the API server. So any config the scenario needs in the
    /// remote case (such as the secrets encryption key) must be supplied to
    /// the *server* — which the `kamu_cli_resource_e2e_test!` macro does by
    /// passing the same `options` (incl. `with_kamu_config`) to the
    /// `run_api_server` harness.
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

        // Make the remote context active so plain commands target it.
        kamu.execute(["context", "use", context_name])
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

    /// The registered remote context name, if this is a remote context.
    pub fn context_name(&self) -> Option<&str> {
        match self {
            Self::Local(_) => None,
            Self::Remote { context_name, .. } => Some(context_name),
        }
    }

    /// `["--context", "<name>"]` for a remote context, empty for local.
    ///
    /// Append this to a resource command's args to exercise the per-command
    /// `--context` override path (mode 2). Never append it to the bare
    /// `context` switcher, which does not accept the flag.
    pub fn context_override_arg(&self) -> Vec<String> {
        match self.context_name() {
            Some(name) => vec!["--context".to_string(), name.to_string()],
            None => Vec::new(),
        }
    }

    /// Collect command arguments into an owned vector.
    ///
    /// No context flag is injected: the active context (set up at construction
    /// for the remote case) already determines the target. For per-invocation
    /// `--context` overriding, append [`ResourceCtx::context_override_arg`] in
    /// the scenario.
    pub fn args<I, S>(&self, args: I) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        args.into_iter().map(Into::into).collect()
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Thin command pass-throughs
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    /// Run a command (against the active context) and assert success.
    /// `expected` regex lines are matched against stderr (the CLI emits its
    /// human-readable status there; stdout carries data output).
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

    /// Run a command (against the active context) and assert failure.
    /// `expected` regex lines are matched against stderr.
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

        self.kamu()
            .execute_with_input(args, manifest.to_string())
            .await
            .success();
    }

    /// Run a command with stdin, asserting success and optional stderr regexes.
    pub async fn assert_success_with_stdin<I, S>(
        &self,
        args: I,
        stdin: &str,
        expected: Option<&[&str]>,
    ) where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        self.kamu()
            .assert_success_command_execution_with_input(
                full,
                stdin.to_string(),
                None,
                expected.map(<[&str]>::to_vec),
            )
            .await;
    }

    /// Run a command (against the active context) and return raw stdout.
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

    /// Run a command (against the active context), assert success, and return
    /// raw stderr. Useful for commands whose status lines are stable but not
    /// emitted in a guaranteed order.
    pub async fn stderr<I, S>(&self, args: I) -> String
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        let result = self.kamu().execute(full).await.success();

        std::str::from_utf8(&result.get_output().stderr)
            .unwrap()
            .to_string()
    }

    /// Apply a canonical `VariableSet` fixture via stdin, asserting success.
    pub async fn apply_variable_set(&self, name: &str, value: &str) {
        let manifest = fixtures::variable_set_manifest_yaml(name, value);
        self.assert_success_with_stdin(["apply", "--stdin"], &manifest, None)
            .await;
    }

    /// Apply a canonical `SecretSet` fixture via stdin, asserting success.
    ///
    /// The caller must wire the e2e fixture with
    /// `fixtures::SECRETS_ENCRYPTION_KAMU_CONFIG`; `SecretSet` encryption
    /// happens inside the CLI/API process that receives the command.
    pub async fn apply_secret_set(&self, name: &str, token: &str, password: &str) {
        let manifest = fixtures::secret_set_manifest_yaml(name, token, password);
        self.assert_success_with_stdin(["apply", "--stdin"], &manifest, None)
            .await;
    }

    /// Assert a resource is present in both `get <kind> <name>` and
    /// `list <kind>` outputs.
    pub async fn assert_resource_present(&self, kind: &str, name: &str) {
        let get_stdout = self
            .stdout(["get".to_string(), kind.to_string(), name.to_string()])
            .await;
        assert!(
            get_stdout.contains(name),
            "expected resource '{name}' to be present, but `get` output did not mention \
             it:\n{get_stdout}"
        );

        let list_stdout = self.stdout(["list".to_string(), kind.to_string()]).await;
        assert!(
            list_stdout.contains(name),
            "expected resource '{name}' present in `list {kind}`, but it did not \
             appear:\n{list_stdout}"
        );
    }

    /// Assert a resource is not present: `get <kind> <name> --ignore-not-found`
    /// succeeds and its name does not appear in `list <kind>`.
    pub async fn assert_resource_absent(&self, kind: &str, name: &str) {
        let get_stdout = self
            .stdout([
                "get".to_string(),
                kind.to_string(),
                name.to_string(),
                "--ignore-not-found".to_string(),
            ])
            .await;
        assert!(
            !get_stdout.contains(name),
            "expected resource '{name}' to be absent, but `get` output mentioned it:\n{get_stdout}"
        );

        let list_stdout = self.stdout(["list".to_string(), kind.to_string()]).await;
        assert!(
            !list_stdout.contains(name),
            "expected resource '{name}' absent from `list {kind}`, but it appeared:\n{list_stdout}"
        );
    }

    /// Fetch a resource as JSON (`get <kind> <name> -o json`) and extract its
    /// stable UID. Looks for a `uid` field anywhere in the parsed document.
    pub async fn resource_uid(&self, kind: &str, name: &str) -> String {
        let stdout = self
            .stdout([
                "get".to_string(),
                kind.to_string(),
                name.to_string(),
                "-o".to_string(),
                "json".to_string(),
            ])
            .await;

        let doc: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
            panic!("`get {kind} {name} -o json` did not return JSON: {e}\n{stdout}")
        });

        find_uid(&doc).unwrap_or_else(|| panic!("no `uid` field found in resource JSON:\n{stdout}"))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Recursively search a JSON value for a `uid` string field.
fn find_uid(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(serde_json::Value::String(uid)) = map.get("uid") {
                return Some(uid.clone());
            }
            map.values().find_map(find_uid)
        }
        serde_json::Value::Array(items) => items.iter().find_map(find_uid),
        _ => None,
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
