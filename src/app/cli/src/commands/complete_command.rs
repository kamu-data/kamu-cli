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
use futures::TryStreamExt;
use glob;
use kamu::domain::*;

use super::{CLIError, Command};
use crate::config::ConfigService;

pub struct CompleteCommand {
    dataset_repo: Option<Arc<dyn DatasetRepository>>,
    remote_repo_reg: Option<Arc<dyn RemoteRepositoryRegistry>>,
    remote_alias_reg: Option<Arc<dyn RemoteAliasesRegistry>>,
    config_service: Arc<ConfigService>,
    cli: clap::Command,
    input: String,
    current: usize,
}

// TODO: This is an extremely hacky way to implement the completion
// but we have to do this until clap supports custom completer functions
impl CompleteCommand {
    pub fn new<S>(
        dataset_repo: Option<Arc<dyn DatasetRepository>>,
        remote_repo_reg: Option<Arc<dyn RemoteRepositoryRegistry>>,
        remote_alias_reg: Option<Arc<dyn RemoteAliasesRegistry>>,
        config_service: Arc<ConfigService>,
        cli: clap::Command,
        input: S,
        current: usize,
    ) -> Self
    where
        S: Into<String>,
    {
        Self {
            dataset_repo,
            remote_repo_reg,
            remote_alias_reg,
            config_service,
            cli,
            input: input.into(),
            current,
        }
    }

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
                writeln!(output, "{}", k).unwrap();
            }
        }
    }

    async fn complete_dataset(&self, output: &mut impl Write, prefix: &str) {
        if let Some(repo) = self.dataset_repo.as_ref() {
            let mut datasets = repo.get_all_datasets();
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
                    writeln!(output, "{}", repo_id).unwrap();
                }
            }
        }
    }

    async fn complete_alias(&self, output: &mut impl Write, prefix: &str) {
        if let Some(repo) = self.dataset_repo.as_ref() {
            if let Some(reg) = self.remote_alias_reg.as_ref() {
                let mut datasets = repo.get_all_datasets();
                while let Some(dataset_handle) = datasets.try_next().await.unwrap() {
                    let aliases = reg
                        .get_remote_aliases(&dataset_handle.as_local_ref())
                        .await
                        .unwrap();
                    for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                        if alias.to_string().starts_with(prefix) {
                            writeln!(output, "{}", alias).unwrap();
                        }
                    }
                    for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                        if alias.to_string().starts_with(prefix) {
                            writeln!(output, "{}", alias).unwrap();
                        }
                    }
                }
            }
        }
    }

    fn complete_config_key(&self, output: &mut impl Write, prefix: &str) {
        for key in self.config_service.all_keys() {
            if key.starts_with(prefix) {
                writeln!(output, "{}", key).unwrap();
            }
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

    pub async fn complete(&mut self, output: &mut impl Write) -> Result<(), CLIError> {
        let mut args = match shlex::split(&self.input) {
            Some(v) => v,
            _ => return Ok(()),
        };

        args.truncate(self.current + 1);

        let mut last_cmd = &self.cli;

        // Establish command context
        for arg in args[1..].iter() {
            for s in last_cmd.get_subcommands() {
                if s.get_name() == *arg || s.get_visible_aliases().find(|a| a == arg).is_some() {
                    last_cmd = s;
                    break;
                }
            }
        }

        let empty = "".to_owned();
        let prev = args.get(self.current - 1).unwrap_or(&empty);
        let to_complete = args.get(self.current).unwrap_or(&empty);

        // Complete option values
        if prev.starts_with("--") {
            for opt in last_cmd.get_opts() {
                let full_name = format!("--{}", opt.get_long().unwrap_or_default());
                if full_name == *prev && opt.get_action().takes_values() {
                    if let Some(val_names) = opt.get_value_names() {
                        for name in val_names {
                            match name.as_str() {
                                "REPO" => self.complete_repository(output, to_complete),
                                "TIME" => self.complete_timestamp(output),
                                "VAR" => self.complete_env_var(output, to_complete),
                                "SRC" => self.complete_path(output, to_complete),
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
        for pos in last_cmd.get_positionals() {
            match pos.get_id().as_str() {
                "dataset" => self.complete_dataset(output, to_complete).await,
                "repository" => self.complete_repository(output, to_complete),
                "alias" => self.complete_alias(output, to_complete).await,
                "manifest" => self.complete_path(output, to_complete),
                "cfgkey" => self.complete_config_key(output, to_complete),
                _ => (),
            }
        }

        // Complete args
        if to_complete.starts_with("-") {
            for arg in last_cmd.get_arguments() {
                let full_name = if let Some(long) = arg.get_long() {
                    format!("--{}", long)
                } else if let Some(short) = arg.get_short() {
                    format!("-{}", short)
                } else {
                    "".into()
                };
                if full_name.starts_with(to_complete) {
                    writeln!(output, "{}", full_name).int_err()?;
                }
            }
        }

        Ok(())
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompleteCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        self.complete(&mut std::io::stdout()).await
    }
}
