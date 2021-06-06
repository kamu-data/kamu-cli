use core::panic;
use std::path::{Path, PathBuf};

use kamu::infra::Manifest;

use merge::Merge;
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use std::fmt::Write;

use crate::error::Error;

const CONFIG_VERSION: i32 = 1;
const CONFIG_FILENAME: &str = ".kamuconfig";

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CLIConfig {
    #[merge(strategy = merge_recursive)]
    engine: Option<EngineConfig>,
}

impl Default for CLIConfig {
    fn default() -> Self {
        Self { engine: None }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[skip_serializing_none]
#[derive(Debug, Clone, Merge, Serialize, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EngineConfig {
    runtime: Option<EngineRuntime>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { runtime: None }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum EngineRuntime {
    Docker,
    Podman,
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

impl ConfigService {
    pub fn new(workspace_kamu_dir: PathBuf) -> Self {
        Self {
            workspace_kamu_dir: workspace_kamu_dir,
        }
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
        assert_eq!(manifest.api_version, CONFIG_VERSION);

        manifest.content
    }

    fn to_raw(&self, config: CLIConfig) -> serde_yaml::Value {
        let s = serde_yaml::to_string(&config).unwrap();
        serde_yaml::from_str(&s).unwrap()
    }

    pub fn load(&self, scope: ConfigScope) -> CLIConfig {
        match scope {
            ConfigScope::Flattened => self.load_flattened(),
            _ => {
                let config_path = &self.path_for_scope(scope);
                if !config_path.exists() {
                    CLIConfig::default()
                } else {
                    self.load_from(config_path)
                }
            }
        }
    }

    pub fn load_flattened(&self) -> CLIConfig {
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

        let mut result = CLIConfig::default();
        for path in to_load {
            let cfg = self.load_from(&path);
            result.merge(cfg);
        }

        result
    }

    pub fn save(&mut self, config: CLIConfig, scope: ConfigScope) {
        let config_path = self.path_for_scope(scope);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(config_path)
            .unwrap();

        let manifest = Manifest {
            api_version: CONFIG_VERSION,
            kind: "CLIConfig".to_owned(),
            content: config,
        };

        serde_yaml::to_writer(file, &manifest).unwrap();
    }

    pub fn get(&self, key: &str, scope: ConfigScope) -> Option<String> {
        let config = self.load(scope);
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

    pub fn set(&mut self, key: &str, value: &str, scope: ConfigScope) -> Result<(), Error> {
        if scope == ConfigScope::Workspace && !self.workspace_kamu_dir.exists() {
            return Err(Error::NotInWorkspace);
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
            serde_yaml::from_str(&buffer).map_err(|e| Error::UsageError { msg: e.to_string() })?;

        let current = self.load(scope);

        delta.merge(current);

        self.save(delta, scope);

        Ok(())
    }

    pub fn unset(&mut self, key: &str, scope: ConfigScope) -> Result<(), Error> {
        if scope == ConfigScope::Workspace && !self.workspace_kamu_dir.exists() {
            return Err(Error::NotInWorkspace);
        }

        let config_path = self.path_for_scope(scope);
        if !config_path.exists() {
            return Err(Error::UsageError {
                msg: format!("Key {} not found", key),
            });
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
                    api_version: CONFIG_VERSION,
                    kind: "CLIConfig".to_owned(),
                    content: config_raw,
                },
            )
            .unwrap();

            Ok(())
        } else {
            Err(Error::UsageError {
                msg: format!("Key {} not found", key),
            })
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

    pub fn list(&self, scope: ConfigScope) -> String {
        let config = self.load(scope);
        let yaml = serde_yaml::to_string(&config).unwrap();
        self.strip_yaml(&yaml).to_owned()
    }

    fn strip_yaml<'a>(&self, yaml_str: &'a str) -> &'a str {
        yaml_str.split_once('\n').unwrap().1.trim_end()
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
