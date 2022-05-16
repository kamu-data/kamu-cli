// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::config::ConfigService;

use super::{CLIError, Command};
use kamu::domain::*;

use chrono::prelude::*;
use glob;
use std::fs;
use std::path;
use std::sync::Arc;

pub struct CompleteCommand {
    dataset_reg: Option<Arc<dyn DatasetRegistry>>,
    remote_repo_reg: Option<Arc<dyn RemoteRepositoryRegistry>>,
    remote_alias_reg: Option<Arc<dyn RemoteAliasesRegistry>>,
    config_service: Arc<ConfigService>,
    cli: clap::Command<'static>,
    input: String,
    current: usize,
}

// TODO: This is an extremely hacky way to implement the completion
// but we have to do this until clap supports custom completer functions
impl CompleteCommand {
    pub fn new(
        dataset_reg: Option<Arc<dyn DatasetRegistry>>,
        remote_repo_reg: Option<Arc<dyn RemoteRepositoryRegistry>>,
        remote_alias_reg: Option<Arc<dyn RemoteAliasesRegistry>>,
        config_service: Arc<ConfigService>,
        cli: clap::Command<'static>,
        input: String,
        current: usize,
    ) -> Self {
        Self {
            dataset_reg,
            remote_repo_reg,
            remote_alias_reg,
            config_service,
            cli,
            input,
            current,
        }
    }

    fn complete_timestamp(&self) {
        println!("{}", Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true));
    }

    fn complete_env_var(&self, prefix: &str) {
        for (k, _) in std::env::vars() {
            if k.starts_with(prefix) {
                println!("{}", k);
            }
        }
    }

    fn complete_dataset(&self, prefix: &str) {
        if let Some(repo) = self.dataset_reg.as_ref() {
            for dataset_handle in repo.get_all_datasets() {
                if dataset_handle.name.starts_with(prefix) {
                    println!("{}", dataset_handle.name);
                }
            }
        }
    }

    fn complete_repository(&self, prefix: &str) {
        if let Some(reg) = self.remote_repo_reg.as_ref() {
            for repo_id in reg.get_all_repositories() {
                if repo_id.starts_with(prefix) {
                    println!("{}", repo_id);
                }
            }
        }
    }

    async fn complete_alias(&self, prefix: &str) {
        if let Some(repo) = self.dataset_reg.as_ref() {
            if let Some(reg) = self.remote_alias_reg.as_ref() {
                for dataset_handle in repo.get_all_datasets() {
                    let aliases = reg
                        .get_remote_aliases(&dataset_handle.as_local_ref())
                        .await
                        .unwrap();
                    for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                        if alias.to_string().starts_with(prefix) {
                            println!("{}", alias);
                        }
                    }
                    for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                        if alias.to_string().starts_with(prefix) {
                            println!("{}", alias);
                        }
                    }
                }
            }
        }
    }

    fn complete_config_key(&self, prefix: &str) {
        for key in self.config_service.all_keys() {
            if key.starts_with(prefix) {
                println!("{}", key);
            }
        }
    }

    fn complete_path(&self, prefix: &str) {
        let path = path::Path::new(prefix);
        let mut matched_dirs = 0;
        let mut last_matched_dir: path::PathBuf = path::PathBuf::new();

        if !path.exists() {
            let mut glb = path.to_str().unwrap().to_owned();
            glb.push('*');

            for entry in glob::glob(&glb).unwrap() {
                let p = entry.unwrap();
                if p.is_dir() {
                    println!("{}{}", p.display(), std::path::MAIN_SEPARATOR);
                    matched_dirs += 1;
                    last_matched_dir = p;
                } else {
                    println!("{}", p.display());
                }
            }
        } else if path.is_dir() {
            for entry in fs::read_dir(path).unwrap() {
                println!("{}", entry.unwrap().path().display());
            }
        }

        // HACK: to prevent a directory from fulfilling the completion fully
        // we add an extra result that should advance the completion
        // but not finish it.
        if matched_dirs == 1 && !last_matched_dir.to_str().unwrap().is_empty() {
            println!(
                "{}{}...",
                last_matched_dir.display(),
                std::path::MAIN_SEPARATOR
            );
        }
    }
}

#[async_trait::async_trait(?Send)]
impl Command for CompleteCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    async fn run(&mut self) -> Result<(), CLIError> {
        let mut args = match shlex::split(&self.input) {
            Some(v) => v,
            _ => return Ok(()),
        };

        args.truncate(self.current + 1);

        let mut last_cmd = &self.cli;

        // Establish command context
        for arg in args[1..].iter() {
            for s in last_cmd.get_subcommands() {
                if s.get_name() == *arg {
                    last_cmd = s;
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
                if full_name == *prev && opt.is_takes_value_set() {
                    if let Some(val_names) = opt.get_value_names() {
                        for name in val_names {
                            match *name {
                                "REPO" => self.complete_repository(to_complete),
                                "TIME" => self.complete_timestamp(),
                                "VAR" => self.complete_env_var(to_complete),
                                "SRC" => self.complete_path(to_complete),
                                _ => (),
                            }
                        }
                    }
                    if let Some(possible_vals) = opt.get_possible_values() {
                        for pval in possible_vals {
                            if pval.get_name().starts_with(to_complete) {
                                println!("{}", pval.get_name());
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }

        // Complete commands
        for s in last_cmd.get_subcommands() {
            if !s.is_hide_set() && s.get_name().starts_with(to_complete) {
                println!("{}", s.get_name());
            }
        }

        // Complete positionals
        for pos in last_cmd.get_positionals() {
            match pos.get_id() {
                "dataset" => self.complete_dataset(to_complete),
                "repository" => self.complete_repository(to_complete),
                "alias" => self.complete_alias(to_complete).await,
                "manifest" => self.complete_path(to_complete),
                "cfgkey" => self.complete_config_key(to_complete),
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
                    println!("{}", full_name);
                }
            }
        }

        Ok(())
    }
}
