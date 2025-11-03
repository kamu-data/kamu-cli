// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::path::{Path, PathBuf};

use dill::*;
use merge::Merge;
use odf::metadata::serde::yaml::Manifest;

use crate::config::models::*;
use crate::error::CLIError;
use crate::{NotInWorkspace, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CONFIG_VERSION: i32 = 1;
pub const CONFIG_FILENAME: &str = ".kamuconfig";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigService {
    workspace_kamu_dir: PathBuf,
}

#[component(pub)]
impl ConfigService {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            workspace_kamu_dir: workspace_layout.root_dir.clone(),
        }
    }

    pub fn load(&self, scope: ConfigScope) -> CLIConfig {
        match scope {
            ConfigScope::Flattened => self.load_flattened(),
            _ => {
                let config_path = &self.path_for_scope(scope);
                if !config_path.exists() {
                    CLIConfig::new()
                } else {
                    self.load_from(config_path)
                }
            }
        }
    }

    pub fn load_with_defaults(&self, scope: ConfigScope) -> CLIConfig {
        let mut config = self.load(scope);
        config.merge(CLIConfig::default());
        config
    }

    fn load_flattened(&self) -> CLIConfig {
        let mut to_load: Vec<PathBuf> = Vec::new();
        let mut current: &Path = &self.workspace_kamu_dir;

        loop {
            let conf = current.join(CONFIG_FILENAME);
            if conf.exists() {
                to_load.push(conf);
            }
            if let Some(parent) = current.parent() {
                current = parent;
            } else {
                break;
            }
        }

        let user_config = self.path_for_scope(ConfigScope::User);
        if user_config.exists() && !to_load.contains(&user_config) {
            to_load.push(user_config);
        }

        let mut result = CLIConfig::new();
        for path in to_load {
            let cfg = self.load_from(&path);
            result.merge(cfg);
        }

        result
    }

    pub fn save(&self, config: CLIConfig, scope: ConfigScope) {
        let config_path = self.path_for_scope(scope);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(config_path)
            .unwrap();

        let manifest = Manifest {
            kind: "CLIConfig".to_owned(),
            version: CONFIG_VERSION,
            content: config,
        };

        serde_yaml::to_writer(file, &manifest).unwrap();
    }

    pub fn get(&self, key: &str, scope: ConfigScope, with_defaults: bool) -> Option<String> {
        let mut config = self.load(scope);
        if with_defaults {
            config.merge(CLIConfig::default());
        }
        let config_raw = self.to_raw(&config);

        let mut current = &config_raw;

        for subkey in key.split('.') {
            if let Some(next) = current.get(subkey) {
                current = next;
            } else {
                return None;
            }
        }

        Some(serde_yaml::to_string(current).unwrap())
    }

    pub fn set(&self, key: &str, value: &str, scope: ConfigScope) -> Result<(), CLIError> {
        if scope == ConfigScope::Workspace && !self.workspace_kamu_dir.exists() {
            return Err(CLIError::usage_error_from(NotInWorkspace));
        }

        let mut buffer = String::new();

        for (nesting, sub_key) in key.split('.').enumerate() {
            if nesting != 0 {
                writeln!(buffer).unwrap();
            }
            for _ in 0..nesting {
                write!(buffer, "  ").unwrap();
            }
            write!(buffer, "{sub_key}:").unwrap();
        }
        write!(buffer, " {value}").unwrap();

        let mut delta: CLIConfig =
            serde_yaml::from_str(&buffer).map_err(|e| CLIError::usage_error(e.to_string()))?;

        let current = self.load(scope);

        delta.merge(current);

        self.save(delta, scope);

        Ok(())
    }

    pub fn unset(&self, key: &str, scope: ConfigScope) -> Result<(), CLIError> {
        if scope == ConfigScope::Workspace && !self.workspace_kamu_dir.exists() {
            return Err(CLIError::usage_error_from(NotInWorkspace));
        }

        let config_path = self.path_for_scope(scope);
        if !config_path.exists() {
            return Err(CLIError::usage_error(format!("Key {key} not found")));
        }

        let config = self.load_from(&config_path);
        let mut config_raw = self.to_raw(&config);

        if self.unset_recursive(key, config_raw.as_mapping_mut().unwrap()) {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(&config_path)
                .unwrap();

            serde_yaml::to_writer(
                file,
                &Manifest {
                    kind: "CLIConfig".to_owned(),
                    version: CONFIG_VERSION,
                    content: config_raw,
                },
            )
            .unwrap();

            Ok(())
        } else {
            Err(CLIError::usage_error(format!("Key {key} not found")))
        }
    }

    #[allow(clippy::self_only_used_in_recursion)]
    fn unset_recursive(&self, key: &str, value: &mut serde_yaml::Mapping) -> bool {
        if let Some((head, tail)) = key.split_once('.') {
            let index = serde_yaml::Value::String(head.to_owned());

            if let Some(child) = value.get_mut(&index).and_then(|v| v.as_mapping_mut())
                && self.unset_recursive(tail, child)
            {
                if child.is_empty() {
                    value.remove(&index);
                }
                return true;
            }
            false
        } else {
            value
                .remove(serde_yaml::Value::String(key.to_owned()))
                .is_some()
        }
    }

    pub fn list(&self, scope: ConfigScope, with_defaults: bool) -> String {
        let mut config = self.load(scope);
        if with_defaults {
            config.merge(CLIConfig::default());
        }
        serde_yaml::to_string(&config).unwrap()
    }

    pub fn all_keys(&self) -> Vec<String> {
        let mut result = Vec::new();
        let full_config = CLIConfig::sample();
        let raw_config = self.to_raw(&full_config);
        self.visit_keys_recursive("", &raw_config, &mut |key| result.push(key));
        result
    }

    #[allow(clippy::self_only_used_in_recursion)]
    fn visit_keys_recursive(
        &self,
        prefix: &str,
        value: &serde_yaml::Value,
        fun: &mut impl FnMut(String),
    ) {
        if let Some(mapping) = value.as_mapping() {
            for (k, v) in mapping {
                if let Some(key) = k.as_str() {
                    let mut full_key = String::with_capacity(prefix.len() + key.len());
                    full_key.push_str(prefix);
                    full_key.push_str(key);

                    full_key.push('.');
                    self.visit_keys_recursive(&full_key, v, fun);

                    full_key.pop();
                    fun(full_key);
                }
            }
        }
    }

    fn path_for_scope(&self, scope: ConfigScope) -> PathBuf {
        match scope {
            // TODO: Respect `XDG_CONFIG_HOME` when working with configs
            //       https://github.com/kamu-data/kamu-cli/issues/848
            ConfigScope::User => dirs::home_dir()
                .expect("Cannot determine user home directory")
                .join(CONFIG_FILENAME),
            ConfigScope::Workspace => self.workspace_kamu_dir.join(CONFIG_FILENAME),
            _ => panic!(),
        }
    }

    fn load_from(&self, config_path: &Path) -> CLIConfig {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(config_path)
            .unwrap();

        let manifest: Manifest<CLIConfig> = serde_yaml::from_reader(file).unwrap();

        // TODO: Migrations
        assert_eq!(manifest.kind, "CLIConfig");
        assert_eq!(manifest.version, CONFIG_VERSION);

        manifest.content
    }

    fn to_raw(&self, config: &CLIConfig) -> serde_yaml::Value {
        let s = serde_yaml::to_string(config).unwrap();
        serde_yaml::from_str(&s).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
