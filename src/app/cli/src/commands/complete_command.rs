// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;
use std::sync::Arc;
use std::{fs, path};

use chrono::prelude::*;
use clap::CommandFactory as _;
use futures::TryStreamExt;
use glob;
use internal_error::ResultIntoInternal;
use kamu::domain::*;
use kamu_datasets::DatasetRegistry;

use super::{CLIError, Command};
use crate::WorkspaceService;
use crate::config::ConfigService;
use crate::resource_context::ResourceContextRegistryService;
use crate::resources::ResourceTypeLookupService;

#[dill::component]
#[dill::interface(dyn Command)]
pub struct CompleteCommand {
    dataset_registry: Option<Arc<dyn DatasetRegistry>>,
    remote_repo_reg: Option<Arc<dyn RemoteRepositoryRegistry>>,
    remote_alias_reg: Option<Arc<dyn RemoteAliasesRegistry>>,
    config_service: Arc<ConfigService>,
    resource_type_lookup_service: Arc<dyn ResourceTypeLookupService>,
    resource_context_registry_service: Arc<ResourceContextRegistryService>,
    workspace_service: Arc<WorkspaceService>,

    #[dill::component(explicit)]
    input: String,

    #[dill::component(explicit)]
    current: usize,
}

// TODO: This is an extremely hacky way to implement the completion
// but we have to do this until clap supports custom completer functions
impl CompleteCommand {
    fn complete_timestamp(&self, output: &mut impl Write) {
        writeln!(
            output,
            "{}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
        )
        .unwrap();
    }

    fn complete_env_var(&self, output: &mut impl Write, prefix: &str) {
        for (k, _) in std::env::vars() {
            if k.starts_with(prefix) {
                writeln!(output, "{k}").unwrap();
            }
        }
    }

    async fn complete_dataset(&self, output: &mut impl Write, prefix: &str) {
        if let Some(registry) = self.dataset_registry.as_ref() {
            let mut datasets = registry.all_dataset_handles();
            while let Some(dataset_handle) = datasets.try_next().await.unwrap() {
                if dataset_handle.alias.dataset_name.starts_with(prefix) {
                    writeln!(output, "{}", dataset_handle.alias).unwrap();
                }
            }
        }
    }

    fn complete_repository(&self, output: &mut impl Write, prefix: &str) {
        if let Some(reg) = self.remote_repo_reg.as_ref() {
            for repo_id in reg.get_all_repositories() {
                if repo_id.starts_with(prefix) {
                    writeln!(output, "{repo_id}").unwrap();
                }
            }
        }
    }

    async fn complete_alias(&self, output: &mut impl Write, prefix: &str) {
        if let Some(registry) = self.dataset_registry.as_ref()
            && let Some(reg) = self.remote_alias_reg.as_ref()
        {
            let mut datasets = registry.all_dataset_handles();
            while let Some(hdl) = datasets.try_next().await.unwrap() {
                let aliases = reg.get_remote_aliases(&hdl).await.unwrap();
                for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                    if alias.to_string().starts_with(prefix) {
                        writeln!(output, "{alias}").unwrap();
                    }
                }
                for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                    if alias.to_string().starts_with(prefix) {
                        writeln!(output, "{alias}").unwrap();
                    }
                }
            }
        }
    }

    fn complete_config_key(&self, output: &mut impl Write, prefix: &str) {
        for path in self.config_service.complete_path(prefix) {
            writeln!(output, "{path}").unwrap();
        }
    }

    async fn complete_resource_type(
        &self,
        output: &mut impl Write,
        prefix: &str,
        extra_targets: &[&str],
        explicit_context_name: Option<&str>,
    ) {
        for target in extra_targets {
            if target.starts_with(prefix) {
                writeln!(output, "{target}").unwrap();
            }
        }

        if let Ok(descriptors) = self
            .resource_type_lookup_service
            .list_supported_resource_types(explicit_context_name)
            .await
        {
            for descriptor in &descriptors {
                if descriptor.canonical_selector.as_str().starts_with(prefix) {
                    writeln!(output, "{}", descriptor.canonical_selector).unwrap();
                }
                for alias in &descriptor.selector_aliases {
                    if alias.as_str().starts_with(prefix) {
                        writeln!(output, "{alias}").unwrap();
                    }
                }
            }
        }
    }

    fn context_names_matching(&self, prefix: &str) -> Vec<String> {
        let mut names = Vec::new();
        if self.workspace_service.is_in_workspace() && "local".starts_with(prefix) {
            names.push("local".to_string());
        }
        for ctx in self
            .resource_context_registry_service
            .list_effective_contexts()
        {
            if ctx.name.starts_with(prefix) {
                names.push(ctx.name);
            }
        }
        names
    }

    fn complete_context_name(&self, output: &mut impl Write, prefix: &str) {
        for name in self.context_names_matching(prefix) {
            writeln!(output, "{name}").unwrap();
        }
    }

    /// Completes a clustered short context option (`-cprod`, `-wcprod`) that
    /// is itself the token being typed, emitting full `<flag_prefix><name>`
    /// candidates since bash replaces the whole fused token, not just the
    /// value fragment.
    fn complete_attached_context_name(
        &self,
        output: &mut impl Write,
        flag_prefix: &str,
        value_prefix: &str,
    ) {
        for name in self.context_names_matching(value_prefix) {
            writeln!(output, "{flag_prefix}{name}").unwrap();
        }
    }

    fn complete_path(&self, output: &mut impl Write, prefix: &str) {
        let path = path::Path::new(prefix);
        let mut matched_dirs = 0;
        let mut last_matched_dir: path::PathBuf = path::PathBuf::new();

        if !path.exists() {
            let mut glb = path.to_str().unwrap().to_owned();
            glb.push('*');

            for entry in glob::glob(&glb).unwrap() {
                let p = entry.unwrap();
                if p.is_dir() {
                    writeln!(output, "{}{}", p.display(), std::path::MAIN_SEPARATOR).unwrap();
                    matched_dirs += 1;
                    last_matched_dir = p;
                } else {
                    writeln!(output, "{}", p.display()).unwrap();
                }
            }
        } else if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                writeln!(output, "{}", entry.unwrap().path().display()).unwrap();
            }
        }

        // HACK: to prevent a directory from fulfilling the completion fully
        // we add an extra result that should advance the completion
        // but not finish it.
        if matched_dirs == 1 && !last_matched_dir.to_str().unwrap().is_empty() {
            writeln!(
                output,
                "{}{}...",
                last_matched_dir.display(),
                std::path::MAIN_SEPARATOR
            )
            .unwrap();
        }
    }

    pub async fn complete(&self, output: &mut impl Write) -> Result<(), CLIError> {
        let Some(mut args) = shlex::split(&self.input) else {
            return Ok(());
        };

        let cli = crate::cli::Cli::command();

        // `current == args.len()` is the normal case of completing a new,
        // still-empty word (e.g. after a trailing space). Anything beyond
        // that (a stale COMP_CWORD) has no token to anchor to.
        if self.current > args.len() {
            return Ok(());
        }

        args.truncate((self.current + 1).min(args.len()));

        let empty = String::new();
        let prev = self
            .current
            .checked_sub(1)
            .and_then(|i| args.get(i))
            .unwrap_or(&empty);
        let to_complete = args.get(self.current).unwrap_or(&empty);

        // Single pass over the tokens before the cursor that: walks past
        // subcommand names (skipping options and their values along the way,
        // so a leading `-v`/`--context foo` etc. doesn't stop subcommand
        // discovery early), counts free (non-option) positional tokens
        // already consumed by the current command (some commands, e.g.
        // `Get.args`, complete differently depending on slot), and captures
        // an explicit `--context`/`-c` value so type completion targets the
        // right backend instead of always the active context.
        let mut last_cmd = &cli;
        let mut free_positionals_before_cursor = 0;
        let mut explicit_context_name = None;
        let mut pending_value_for: Option<&str> = None;
        // Bash's default `COMP_WORDBREAKS` includes `=`, so on the real
        // completion path `--context=prod` arrives as separate `--context`,
        // `=`, `prod` tokens; a lone `=` immediately after a long option
        // that's awaiting its value is that split, not a fresh token.
        for arg in args.iter().take(self.current).skip(1) {
            if let Some(long) = pending_value_for.take() {
                if arg == "=" {
                    pending_value_for = Some(long);
                    continue;
                }
                if long == "context" {
                    explicit_context_name = Some(arg.as_str());
                }
                continue;
            }
            if let Some(rest) = arg.strip_prefix("--") {
                let (long, attached_value) = match rest.split_once('=') {
                    Some((long, value)) => (long, Some(value)),
                    None => (rest, None),
                };
                if let Some(opt) = last_cmd.get_opts().find(|o| o.get_long() == Some(long))
                    && opt.get_action().takes_values()
                {
                    if let Some(value) = attached_value {
                        if long == "context" {
                            explicit_context_name = Some(value);
                        }
                    } else {
                        pending_value_for = Some(long);
                    }
                }
                continue;
            }
            if let Some(cluster) = arg.strip_prefix('-').filter(|s| !s.is_empty()) {
                // Short options can cluster (`-wcprod` == `-w -c prod`), so
                // walk each flag in turn until one consumes the remainder as
                // its value.
                for (idx, c) in cluster.char_indices() {
                    let Some(opt) = last_cmd.get_opts().find(|o| o.get_short() == Some(c)) else {
                        break;
                    };
                    if opt.get_action().takes_values() {
                        let attached_value = &cluster[idx + c.len_utf8()..];
                        if !attached_value.is_empty() {
                            if opt.get_long() == Some("context") {
                                explicit_context_name = Some(attached_value);
                            }
                        } else {
                            pending_value_for = opt.get_long();
                        }
                        break;
                    }
                }
                continue;
            }

            let mut matched_subcommand = false;
            for s in last_cmd.get_subcommands() {
                if s.get_name() == arg.as_str() || s.get_visible_aliases().any(|a| a == arg) {
                    last_cmd = s;
                    matched_subcommand = true;
                    break;
                }
            }
            if !matched_subcommand {
                free_positionals_before_cursor += 1;
            }
        }

        // At exactly `--context=<TAB>`, bash makes `=` the current word.
        // It retains the option prefix when replacing `=`, so candidates
        // must be bare context names.
        if to_complete == "="
            && let Some(long) = prev.strip_prefix("--")
            && long == "context"
            && last_cmd.get_opts().any(|o| o.get_long() == Some("context"))
        {
            self.complete_context_name(output, "");
            return Ok(());
        }

        // Once a value prefix exists, bash splits `--context=pr` into
        // `--context`, `=`, `pr`; only the bare `pr` fragment is replaced.
        if prev == "="
            && self.current >= 2
            && let Some(long) = args.get(self.current - 2)
            && let Some(long) = long.strip_prefix("--")
            && long == "context"
            && last_cmd.get_opts().any(|o| o.get_long() == Some("context"))
        {
            self.complete_context_name(output, to_complete);
            return Ok(());
        }
        // A clustered short option contains no break character (`-cpr`,
        // `-wcpr`), so it stays fused as one token even under
        // `COMP_WORDBREAKS`; here the whole cluster is `to_complete` and
        // bash will replace it wholesale, so the candidate must be the full
        // `-cNAME` spelling, not just the bare name.
        if let Some(cluster) = to_complete.strip_prefix('-').filter(|s| !s.is_empty())
            && !cluster.starts_with('-')
        {
            for (idx, c) in cluster.char_indices() {
                let Some(opt) = last_cmd.get_opts().find(|o| o.get_short() == Some(c)) else {
                    break;
                };
                if opt.get_action().takes_values() {
                    if opt.get_long() == Some("context") {
                        let value = &cluster[idx + c.len_utf8()..];
                        let flag_prefix = format!("-{}", &cluster[..idx + c.len_utf8()]);
                        self.complete_attached_context_name(output, &flag_prefix, value);
                        return Ok(());
                    }
                    break;
                }
            }
        }

        // Complete option values
        if prev.starts_with('-') {
            for opt in last_cmd.get_opts() {
                let long_name = format!("--{}", opt.get_long().unwrap_or_default());
                let short_name = opt.get_short().map(|c| format!("-{c}"));
                let is_match = long_name == *prev || short_name.as_deref() == Some(prev.as_str());
                if is_match && opt.get_action().takes_values() {
                    if let Some(val_names) = opt.get_value_names() {
                        for name in val_names {
                            match name.as_str() {
                                "REPO" => self.complete_repository(output, to_complete),
                                "TIME" => self.complete_timestamp(output),
                                "VAR" => self.complete_env_var(output, to_complete),
                                "FILE" => self.complete_path(output, to_complete),
                                "CONTEXT_NAME" => self.complete_context_name(output, to_complete),
                                _ => (),
                            }
                        }
                    }
                    for pval in opt.get_possible_values() {
                        if pval.get_name().starts_with(to_complete) {
                            writeln!(output, "{}", pval.get_name()).int_err()?;
                        }
                    }
                    return Ok(());
                }
            }
        }

        // Complete commands
        for s in last_cmd.get_subcommands() {
            if !s.is_hide_set() && s.get_name().starts_with(to_complete) {
                writeln!(output, "{}", s.get_name()).int_err()?;
            }
        }

        // Complete positionals
        let cmd_name = last_cmd.get_name();
        for pos in last_cmd.get_positionals() {
            match pos.get_id().as_str() {
                "alias" => self.complete_alias(output, to_complete).await,
                "cfgkey" => self.complete_config_key(output, to_complete),
                "dataset" => self.complete_dataset(output, to_complete).await,
                "file" | "manifest" => self.complete_path(output, to_complete),
                "repository" => self.complete_repository(output, to_complete),
                "target"
                    if (cmd_name == "list" || cmd_name == "delete")
                        && free_positionals_before_cursor == 0 =>
                {
                    self.complete_resource_type(
                        output,
                        to_complete,
                        &["datasets", "all"],
                        explicit_context_name,
                    )
                    .await;
                }
                // `Get.args` is `type name...`: only the first token is a type selector.
                "args" if cmd_name == "get" && free_positionals_before_cursor == 0 => {
                    self.complete_resource_type(output, to_complete, &[], explicit_context_name)
                        .await;
                }
                // Only positionals that take an *existing* context name use
                // this id; `ContextAdd` names its field `new_name` instead.
                "name" => self.complete_context_name(output, to_complete),
                _ => (),
            }
        }

        // Complete args
        if to_complete.starts_with('-') {
            for arg in last_cmd.get_arguments() {
                let full_name = if let Some(long) = arg.get_long() {
                    format!("--{long}")
                } else if let Some(short) = arg.get_short() {
                    format!("-{short}")
                } else {
                    String::new()
                };
                if full_name.starts_with(to_complete) {
                    writeln!(output, "{full_name}").int_err()?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompleteCommand {
    async fn run(&self) -> Result<(), CLIError> {
        self.complete(&mut std::io::stdout()).await
    }
}
