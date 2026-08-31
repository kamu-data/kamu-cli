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

use super::complete_command_plan::{
    Completion,
    PositionalKind,
    plan_completion,
    resolve_active_command,
};
use super::{CLIError, Command};
use crate::WorkspaceService;
use crate::config::ConfigService;
use crate::resource_context::{LOCAL_CONTEXT_NAME, ResourceContextRegistryService};
use crate::resources::{ANY_SELECTOR, DATASET_TARGET, DATASETS_TARGET, ResourceTypeLookupService};

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
    fn complete_timestamp(&self, output: &mut impl Write) -> Result<(), CLIError> {
        writeln!(
            output,
            "{}",
            Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
        )
        .int_err()?;

        Ok(())
    }

    fn complete_env_var(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        for (k, _) in std::env::vars() {
            if k.starts_with(prefix) {
                writeln!(output, "{k}").int_err()?;
            }
        }

        Ok(())
    }

    async fn complete_dataset(
        &self,
        output: &mut impl Write,
        prefix: &str,
    ) -> Result<(), CLIError> {
        if let Some(registry) = self.dataset_registry.as_ref() {
            let mut datasets = registry.all_dataset_handles();
            while let Some(dataset_handle) = datasets.try_next().await.int_err()? {
                if dataset_handle.alias.dataset_name.starts_with(prefix) {
                    writeln!(output, "{}", dataset_handle.alias).int_err()?;
                }
            }
        }

        Ok(())
    }

    fn complete_repository(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        if let Some(reg) = self.remote_repo_reg.as_ref() {
            for repo_id in reg.get_all_repositories() {
                if repo_id.starts_with(prefix) {
                    writeln!(output, "{repo_id}").int_err()?;
                }
            }
        }

        Ok(())
    }

    async fn complete_alias(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        if let Some(registry) = self.dataset_registry.as_ref()
            && let Some(reg) = self.remote_alias_reg.as_ref()
        {
            let mut datasets = registry.all_dataset_handles();
            while let Some(hdl) = datasets.try_next().await.int_err()? {
                let aliases = reg.get_remote_aliases(&hdl).await.int_err()?;
                for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                    if alias.to_string().starts_with(prefix) {
                        writeln!(output, "{alias}").int_err()?;
                    }
                }
                for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                    if alias.to_string().starts_with(prefix) {
                        writeln!(output, "{alias}").int_err()?;
                    }
                }
            }
        }

        Ok(())
    }

    fn complete_config_key(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        for path in self.config_service.complete_path(prefix) {
            writeln!(output, "{path}").int_err()?;
        }

        Ok(())
    }

    async fn complete_resource_type(
        &self,
        output: &mut impl Write,
        prefix: &str,
        extra_targets: &[&str],
    ) -> Result<(), CLIError> {
        for target in extra_targets {
            if target.starts_with(prefix) {
                writeln!(output, "{target}").int_err()?;
            }
        }

        // There is no local context to resolve outside a workspace.
        if !self.workspace_service.is_in_workspace() {
            return Ok(());
        }

        // Pinned to the local context so completion never blocks on the
        // network: a remote one would issue a GraphQL round-trip per TAB
        // press. `None` would instead follow the active context, which may
        // itself be remote.
        if let Ok(descriptors) = self
            .resource_type_lookup_service
            .list_supported_resource_types(Some(LOCAL_CONTEXT_NAME))
            .await
        {
            // Every accepted spelling: the canonical schema name and its
            // aliases all parse, so offering a subset would hide a valid form.
            for descriptor in &descriptors {
                if descriptor.canonical_selector.as_str().starts_with(prefix) {
                    writeln!(output, "{}", descriptor.canonical_selector).int_err()?;
                }
                for alias in &descriptor.selector_aliases {
                    if alias.as_str().starts_with(prefix) {
                        writeln!(output, "{alias}").int_err()?;
                    }
                }
            }
        }

        Ok(())
    }

    fn complete_context_name(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        // `local` is implicit rather than registered.
        if self.workspace_service.is_in_workspace() && LOCAL_CONTEXT_NAME.starts_with(prefix) {
            writeln!(output, "{LOCAL_CONTEXT_NAME}").int_err()?;
        }

        for sc in self
            .resource_context_registry_service
            .list_effective_contexts_with_scope()
        {
            if sc.context.name.starts_with(prefix) {
                writeln!(output, "{}", sc.context.name).int_err()?;
            }
        }

        Ok(())
    }

    fn complete_path(&self, output: &mut impl Write, prefix: &str) -> Result<(), CLIError> {
        let path = path::Path::new(prefix);
        let mut matched_dirs = 0;
        let mut last_matched_dir: path::PathBuf = path::PathBuf::new();

        if !path.exists() {
            let mut glb = path.to_str().unwrap().to_owned();
            glb.push('*');

            for entry in glob::glob(&glb).int_err()? {
                let p = entry.int_err()?;
                if p.is_dir() {
                    writeln!(output, "{}{}", p.display(), std::path::MAIN_SEPARATOR).int_err()?;
                    matched_dirs += 1;
                    last_matched_dir = p;
                } else {
                    writeln!(output, "{}", p.display()).int_err()?;
                }
            }
        } else if path.is_dir() {
            for entry in fs::read_dir(path).int_err()? {
                writeln!(output, "{}", entry.int_err()?.path().display()).int_err()?;
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
            .int_err()?;
        }

        Ok(())
    }

    pub async fn complete(&self, output: &mut impl Write) -> Result<(), CLIError> {
        let Some(args) = shlex::split(&self.input) else {
            return Ok(());
        };

        // `complete_command_plan` decides what to offer; this only fetches it.
        let plan = plan_completion(&args, self.current);
        let to_complete = plan.to_complete.as_str();

        let cli = crate::cli::Cli::command();
        let last_cmd = resolve_active_command(&cli, &args, self.current);

        for completion in &plan.completions {
            match completion {
                Completion::Subcommands => {
                    for s in last_cmd.get_subcommands() {
                        if !s.is_hide_set() && s.get_name().starts_with(to_complete) {
                            writeln!(output, "{}", s.get_name()).int_err()?;
                        }
                    }
                }

                Completion::OptionValue(value_name) => match value_name.as_str() {
                    "REPO" => self.complete_repository(output, to_complete)?,
                    "TIME" => self.complete_timestamp(output)?,
                    "VAR" => self.complete_env_var(output, to_complete)?,
                    "FILE" => self.complete_path(output, to_complete)?,
                    "CONTEXT_NAME" => self.complete_context_name(output, to_complete)?,
                    _ => (),
                },

                Completion::OptionPossibleValues(arg_id) => {
                    if let Some(opt) = last_cmd.get_opts().find(|o| o.get_id() == arg_id) {
                        for pval in opt.get_possible_values() {
                            if pval.get_name().starts_with(to_complete) {
                                writeln!(output, "{}", pval.get_name()).int_err()?;
                            }
                        }
                    }
                }

                Completion::Positional(kind) => match kind {
                    PositionalKind::Alias => self.complete_alias(output, to_complete).await?,
                    PositionalKind::ConfigKey => self.complete_config_key(output, to_complete)?,
                    PositionalKind::Dataset => self.complete_dataset(output, to_complete).await?,
                    PositionalKind::Path => self.complete_path(output, to_complete)?,
                    PositionalKind::Repository => self.complete_repository(output, to_complete)?,
                    PositionalKind::ContextName => {
                        self.complete_context_name(output, to_complete)?
                    }
                    PositionalKind::ResourceType { with_extra_targets } => {
                        let extra_targets: &[&str] = if *with_extra_targets {
                            &[DATASET_TARGET, DATASETS_TARGET, ANY_SELECTOR]
                        } else {
                            &[]
                        };
                        self.complete_resource_type(output, to_complete, extra_targets)
                            .await?;
                    }
                },

                Completion::OptionNames => {
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
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompleteCommand {
    async fn run(&self) -> Result<(), CLIError> {
        // The completion script calls this on every Tab press and reads it through a
        // command substitution, so the consumer can go away mid-write. Rust ignores
        // SIGPIPE, so the write fails with `BrokenPipe` error instead. `pipecheck` lets
        // the signal terminate the process instead or failing with internal
        // error
        self.complete(&mut pipecheck::wrap(std::io::stdout())).await
    }
}
