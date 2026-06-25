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

use super::{fixtures, summary_count};

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

    /// Mutable access to the underlying CLI puppet for harness-level state
    /// changes such as switching the active account.
    pub fn kamu_mut(&mut self) -> &mut KamuCliPuppet {
        match self {
            Self::Local(kamu) | Self::Remote { kamu, .. } => kamu,
        }
    }

    /// Create an account in the current CLI workspace.
    pub async fn create_account(&self, account_name: &odf::AccountName) {
        self.kamu().create_account(account_name).await;
    }

    /// Switch the active account used by subsequent CLI invocations.
    pub fn set_account(&mut self, account_name: Option<odf::AccountName>) {
        self.kamu_mut().set_account(account_name);
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

    /// Collect command arguments and append `--context <context_name>`.
    ///
    /// This is for scenarios that intentionally exercise per-command context
    /// overrides, including commands targeting a remote context while the
    /// active context remains `local`.
    pub fn args_with_context<I, S>(&self, args: I, context_name: &str) -> Vec<String>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut full = self.args(args);
        full.push("--context".to_string());
        full.push(context_name.to_string());
        full
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

    /// Write `content` to `<workspace>/<rel_path>`, creating parent directories
    /// as needed, and return the absolute path to the written file.
    ///
    /// For path- and directory-based `apply` tests (the stdin helpers cover the
    /// stdin mode). Commands run with `current_dir` set to the workspace, so a
    /// scenario may pass either the returned absolute path or `rel_path` itself
    /// to `apply`.
    pub fn write_manifest(&self, rel_path: &str, content: &str) -> std::path::PathBuf {
        let path = self.kamu().workspace_path().join(rel_path);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        std::fs::write(&path, content).unwrap();
        path
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

    /// Run a command with stdin, asserting failure and optional stderr regexes.
    pub async fn assert_failure_with_stdin<I, S>(
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
            .assert_failure_command_execution_with_input(
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

    /// Run a command and parse stdout as JSON.
    pub async fn stdout_json<I, S>(&self, args: I) -> serde_json::Value
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        let label = full.join(" ");
        let raw = self.stdout(full).await;

        serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("`{label}` did not return JSON: {e}\n{raw}"))
    }

    /// Run a command, parse stdout as YAML, and convert it to a JSON value for
    /// ergonomic field assertions via `pointer`.
    pub async fn stdout_yaml_as_json<I, S>(&self, args: I) -> serde_json::Value
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let full = self.args(args);
        let label = full.join(" ");
        let raw = self.stdout(full).await;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&raw)
            .unwrap_or_else(|e| panic!("`{label}` did not return YAML: {e}\n{raw}"));

        serde_json::to_value(yaml).unwrap_or_else(|e| {
            panic!("`{label}` YAML could not be converted to JSON value: {e}\n{raw}")
        })
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

    /// Assert that the active resource context is exactly `local`.
    ///
    /// Uses `context list -o json`, which *explicitly* requests JSON (bare
    /// `context` only emits JSON as a non-TTY fallback — under a real terminal
    /// it renders a table, so parsing it would be format-fragile). Each entry
    /// carries a `Current` marker (`"*"` for the active context); we assert the
    /// active one is named exactly `local`. A substring match would be too
    /// loose, and matching all listed names would not isolate the active one.
    pub async fn assert_active_context_is_local(&self) {
        let stdout = self.stdout(["context", "list", "-o", "json"]).await;

        let contexts: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
            panic!("`context list -o json` did not return JSON: {e}\n{stdout}")
        });

        let active_names: Vec<&str> = contexts
            .as_array()
            .unwrap_or_else(|| panic!("`context list -o json` should be a JSON array:\n{stdout}"))
            .iter()
            .filter(|entry| entry.get("Current").and_then(|c| c.as_str()) == Some("*"))
            .filter_map(|entry| entry.get("Name").and_then(|n| n.as_str()))
            .collect();

        assert_eq!(
            active_names,
            ["local"],
            "exactly one context should be active and it should be `local`, got:\n{stdout}"
        );
    }

    /// Fetch a resource as JSON (`get <kind> <name> -o json`) and extract its
    /// stable UID. Looks for a `uid` field anywhere in the parsed document.
    pub async fn resource_uid(&self, kind: &str, name: &str) -> String {
        self.get_one(["get", kind, name]).await.uid()
    }

    /// Run `list <kind> -o json` and return the sorted resource names.
    pub async fn list_names(&self, kind: &str) -> Vec<String> {
        let label = format!("list {kind} -o json");
        let doc = self.stdout_json(["list", kind, "-o", "json"]).await;

        let mut names: Vec<String> = doc
            .as_array()
            .unwrap_or_else(|| panic!("`{label}` should be a JSON array:\n{doc}"))
            .iter()
            .map(|row| {
                row.get("Name")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_else(|| panic!("`{label}` row has no Name:\n{row}"))
                    .to_string()
            })
            .collect();
        names.sort();
        names
    }

    /// Return the `summary -o json` total count for a resource kind.
    pub async fn summary_count(&self, kind: &str) -> u64 {
        let doc = self.stdout_json(["summary", "-o", "json"]).await;
        summary_count(&doc, "summary -o json", kind)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
