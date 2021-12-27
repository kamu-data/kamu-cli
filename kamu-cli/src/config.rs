// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::panic;
use std::path::{Path, PathBuf};

use container_runtime::{ContainerRuntimeType, NetworkNamespaceType};
use kamu::infra::Manifest;
use kamu::infra::WorkspaceLayout;

use dill::*;
use duration_string::DurationString;
use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fmt::Write;

use crate::error::CLIError;
use crate::NotInWorkspace;

////////////////////////////////////////////////////////////////////////////////////////

const CONFIG_VERSION: i32 = 1;
const CONFIG_FILENAME: &str = ".kamuconfig";

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CLIConfig {
    #[merge(strategy = merge_recursive)]
    pub engine: Option<EngineConfig>,
}

impl CLIConfig {
    pub fn new() -> Self {
        Self { engine: None }
    }

    // TODO: Remove this workaround
    // Returns config with all values set to non-None
    // This is used to walk the key tree where values that default to None would
    // otherwise be omitted
    fn sample() -> Self {
        Self {
            engine: Some(EngineConfig::sample()),
        }
    }
}

impl Default for CLIConfig {
    fn default() -> Self {
        Self {
            engine: Some(EngineConfig::default()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EngineConfig {
    /// Maximum number of engine operations that can be performed concurrently
    pub max_concurrency: Option<u32>,
    /// Type of the runtime to use when running the data processing engines
    pub runtime: Option<ContainerRuntimeType>,
    /// Type of the networking namespace (relevant when running in container environments)
    pub network_ns: Option<NetworkNamespaceType>,
    /// Timeout for starting an engine container
    pub start_timeout: Option<DurationString>,
    /// Timeout for waiting the engine container to stop gracefully
    pub shutdown_timeout: Option<DurationString>,
}

impl EngineConfig {
    pub fn new() -> Self {
        Self {
            max_concurrency: None,
            runtime: None,
            network_ns: None,
            start_timeout: None,
            shutdown_timeout: None,
        }
    }

    fn sample() -> Self {
        Self {
            max_concurrency: Some(0),
            ..Self::default()
        }
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            max_concurrency: None,
            runtime: Some(ContainerRuntimeType::Docker),
            network_ns: Some(NetworkNamespaceType::Private),
            start_timeout: Some(DurationString::from_string("30s".to_owned()).unwrap()),
            shutdown_timeout: Some(DurationString::from_string("5s".to_owned()).unwrap()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigScope {
    User,
    Workspace,
    Flattened,
}

pub struct ConfigService {
    workspace_kamu_dir: PathBuf,
}

#[component(pub)]
impl ConfigService {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            workspace_kamu_dir: workspace_layout.kamu_root_dir.clone(),
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
                to_load.push(conf)
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
            config.merge(CLIConfig::default())
        }
        let config_raw = self.to_raw(config);

        let mut current = &config_raw;

        for subkey in key.split('.') {
            if let Some(next) = current.get(subkey) {
                current = next;
            } else {
                return None;
            }
        }

        let yaml_str = serde_yaml::to_string(current).unwrap();
        let result = self.strip_yaml(&yaml_str).to_owned();

        return Some(result);
    }

    pub fn set(&self, key: &str, value: &str, scope: ConfigScope) -> Result<(), CLIError> {
        if scope == ConfigScope::Workspace && !self.workspace_kamu_dir.exists() {
            return Err(CLIError::usage_error_from(NotInWorkspace));
        }

        let mut buffer = String::new();

        let mut nesting = 0;
        for subkey in key.split('.') {
            if nesting != 0 {
                write!(buffer, "\n").unwrap();
            }
            for _ in 0..nesting {
                write!(buffer, "  ").unwrap();
            }
            write!(buffer, "{}:", subkey).unwrap();
            nesting += 1;
        }
        write!(buffer, " {}", value).unwrap();

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
            return Err(CLIError::usage_error(format!("Key {} not found", key)));
        }

        let config = self.load_from(&config_path);
        let mut config_raw = self.to_raw(config);

        if self.unset_recursive(key, &mut config_raw.as_mapping_mut().unwrap()) {
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
            Err(CLIError::usage_error(format!("Key {} not found", key)))
        }
    }

    fn unset_recursive(&self, key: &str, value: &mut serde_yaml::Mapping) -> bool {
        if let Some((head, tail)) = key.split_once('.') {
            let index = serde_yaml::Value::String(head.to_owned());

            if let Some(child) = value.get_mut(&index).and_then(|v| v.as_mapping_mut()) {
                if self.unset_recursive(tail, child) {
                    if child.is_empty() {
                        value.remove(&index);
                    }
                    return true;
                }
            }
            return false;
        } else {
            value
                .remove(&serde_yaml::Value::String(key.to_owned()))
                .is_some()
        }
    }

    pub fn list(&self, scope: ConfigScope, with_defaults: bool) -> String {
        let mut config = self.load(scope);
        if with_defaults {
            config.merge(CLIConfig::default())
        }
        let yaml = serde_yaml::to_string(&config).unwrap();
        self.strip_yaml(&yaml).to_owned()
    }

    pub fn all_keys(&self) -> Vec<String> {
        let mut result = Vec::new();
        let full_config = CLIConfig::sample();
        let raw_config = self.to_raw(full_config);
        self.visit_keys_recursive("", &raw_config, &mut |key| result.push(key));
        result
    }

    fn visit_keys_recursive(
        &self,
        prefix: &str,
        value: &serde_yaml::Value,
        fun: &mut impl FnMut(String),
    ) {
        if let Some(mapping) = value.as_mapping() {
            for (k, v) in mapping.iter() {
                if let Some(key) = k.as_str() {
                    let mut full_key = String::with_capacity(prefix.len() + key.len());
                    full_key.push_str(prefix);
                    full_key.push_str(key);

                    full_key.push_str(".");
                    self.visit_keys_recursive(&full_key, v, fun);

                    full_key.pop();
                    fun(full_key);
                }
            }
        }
    }

    fn strip_yaml<'a>(&self, yaml_str: &'a str) -> &'a str {
        yaml_str.split_once('\n').unwrap().1.trim_end()
    }

    fn path_for_scope(&self, scope: ConfigScope) -> PathBuf {
        match scope {
            ConfigScope::User => dirs::home_dir()
                .expect("Cannot determine user home directory")
                .join(&CONFIG_FILENAME),
            ConfigScope::Workspace => self.workspace_kamu_dir.join(&CONFIG_FILENAME),
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

    fn to_raw(&self, config: CLIConfig) -> serde_yaml::Value {
        let s = serde_yaml::to_string(&config).unwrap();
        serde_yaml::from_str(&s).unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

// For some reason merge crate does not recursively merge values inside `Option`
fn merge_recursive<T>(left: &mut Option<T>, right: Option<T>)
where
    T: Merge,
{
    if left.is_none() && right.is_some() {
        left.replace(right.unwrap());
    } else if left.is_some() && right.is_some() {
        left.as_mut().unwrap().merge(right.unwrap());
    }
}
