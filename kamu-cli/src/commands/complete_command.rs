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
    metadata_repo: Option<Arc<dyn MetadataRepository>>,
    config_service: Arc<ConfigService>,
    app: clap::App<'static, 'static>,
    input: String,
    current: usize,
}

// TODO: This is an extremely hacky way to implement the completion
// but we have to do this until clap supports custom completer functions
impl CompleteCommand {
    pub fn new(
        metadata_repo: Option<Arc<dyn MetadataRepository>>,
        config_service: Arc<ConfigService>,
        app: clap::App<'static, 'static>,
        input: String,
        current: usize,
    ) -> Self {
        Self {
            metadata_repo,
            config_service,
            app,
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
        if let Some(repo) = self.metadata_repo.as_ref() {
            for dataset_id in repo.get_all_datasets() {
                if dataset_id.starts_with(prefix) {
                    println!("{}", dataset_id);
                }
            }
        }
    }

    fn complete_repository(&self, prefix: &str) {
        if let Some(repo) = self.metadata_repo.as_ref() {
            for repo_id in repo.get_all_repositories() {
                if repo_id.starts_with(prefix) {
                    println!("{}", repo_id);
                }
            }
        }
    }

    fn complete_alias(&self, prefix: &str) {
        if let Some(repo) = self.metadata_repo.as_ref() {
            for dataset_id in repo.get_all_datasets() {
                let aliases = repo.get_remote_aliases(&dataset_id).unwrap();
                for alias in aliases.get_by_kind(RemoteAliasKind::Pull) {
                    if alias.starts_with(prefix) {
                        println!("{}", alias);
                    }
                }
                for alias in aliases.get_by_kind(RemoteAliasKind::Push) {
                    if alias.starts_with(prefix) {
                        println!("{}", alias);
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

impl Command for CompleteCommand {
    fn needs_workspace(&self) -> bool {
        false
    }

    fn run(&mut self) -> Result<(), CLIError> {
        let mut args = match shlex::split(&self.input) {
            Some(v) => v,
            _ => return Ok(()),
        };

        args.truncate(self.current + 1);

        let mut last_cmd = &self.app.p;

        // Establish command context
        for arg in args[1..].iter() {
            for s in last_cmd.subcommands.iter() {
                if s.p.meta.name == *arg {
                    last_cmd = &s.p;
                }
            }
        }

        let empty = "".to_owned();
        let prev = args.get(self.current - 1).unwrap_or(&empty);
        let to_complete = args.get(self.current).unwrap_or(&empty);

        // Complete option values
        if prev.starts_with("--") {
            for opt in last_cmd.opts.iter() {
                let full_name = format!("--{}", opt.s.long.unwrap());
                if full_name == *prev && opt.b.is_set(clap::ArgSettings::TakesValue) {
                    if let Some(val_names) = &opt.v.val_names {
                        for (_, name) in val_names.iter() {
                            match *name {
                                "REPO" => self.complete_repository(to_complete),
                                "TIME" => self.complete_timestamp(),
                                "VAR" => self.complete_env_var(to_complete),
                                "SRC" => self.complete_path(to_complete),
                                _ => (),
                            }
                        }
                    }
                    if let Some(possible_vals) = &opt.v.possible_vals {
                        for pval in possible_vals {
                            if pval.starts_with(to_complete) {
                                println!("{}", pval);
                            }
                        }
                    }
                    return Ok(());
                }
            }
        }

        // Complete commands
        for s in last_cmd.subcommands.iter() {
            if !s.p.is_set(clap::AppSettings::Hidden) && s.p.meta.name.starts_with(to_complete) {
                println!("{}", s.p.meta.name);
            }
        }

        // Complete positionals
        for pos in last_cmd.positionals.iter() {
            match pos.1.b.name {
                "dataset" => self.complete_dataset(to_complete),
                "repository" => self.complete_repository(to_complete),
                "alias" => self.complete_alias(to_complete),
                "manifest" => self.complete_path(to_complete),
                "cfgkey" => self.complete_config_key(to_complete),
                _ => (),
            }
        }

        // Complete flags and options
        if to_complete.starts_with("-") {
            if "--help".starts_with(to_complete) {
                println!("--help");
            }
            for flg in last_cmd
                .flags
                .iter()
                .filter(|f| !f.b.is_set(clap::ArgSettings::Hidden))
            {
                let full_name = if flg.s.long.is_some() {
                    format!("--{}", flg.s.long.unwrap())
                } else {
                    format!("-{}", flg.s.short.unwrap())
                };
                if full_name.starts_with(to_complete) {
                    println!("{}", full_name);
                }
            }
            for opt in last_cmd.opts.iter() {
                let full_name = format!("--{}", opt.s.long.unwrap());
                if full_name.starts_with(to_complete) {
                    println!("{}", full_name);
                }
            }
        }

        Ok(())
    }
}
