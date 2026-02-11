// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;

use internal_error::*;
use odf::metadata::serde::yaml::Manifest;
use setty::format::Format;

use crate::config::models::*;
use crate::error::CLIError;
use crate::{NotInWorkspace, WorkspaceLayout};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const CONFIG_VERSION: i32 = 1;
pub const CONFIG_FILENAME: &str = ".kamuconfig";
const CONFIG_PATH_ENV_VAR: &str = "KAMU_CONFIG";
const CONFIG_PREFIX_ENV_VAR: &str = "KAMU_CONFIG__";
const CONFIG_SEPARATOR_ENV_VAR: &str = "__";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum ConfigObjectFormat {
    Yaml,
    Json,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(setty::Config, Copy, clap::ValueEnum)]
pub enum ConfigScope {
    /// Includes only config in user home directory
    User,

    /// Includes only current workspace config
    Workspace,

    /// Includes configs in workspace, parent directories, and user home dir
    Combined,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct ConfigService {
    workspace_kamu_dir: PathBuf,
}

#[dill::component(pub)]
impl ConfigService {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        Self {
            workspace_kamu_dir: workspace_layout.root_dir.clone(),
        }
    }

    pub fn load(&self, scope: ConfigScope) -> Result<CLIConfig, CLIError> {
        let paths = self.read_paths_for_scope(scope);
        self.load_from(&paths)
    }

    pub fn load_from(&self, paths: &[PathBuf]) -> Result<CLIConfig, CLIError> {
        let fig = self.get_config_object(paths);
        let mut cfg = fig.extract().map_err(CLIError::usage_error_from)?;

        // TODO: Consider how to incorporate this into `setty`
        cfg.engine.datafusion_embedded.merge_with_defaults();

        Ok(cfg)
    }

    pub fn list(
        &self,
        scope: ConfigScope,
        with_defaults: bool,
        output_format: ConfigObjectFormat,
    ) -> Result<String, CLIError> {
        let paths = self.read_paths_for_scope(scope);
        let fig = self.get_config_object(&paths);
        let data = fig.data(with_defaults).int_err()?;

        let data = match output_format {
            ConfigObjectFormat::Yaml => setty::format::Yaml::serialize(&data).int_err()?,
            ConfigObjectFormat::Json => setty::format::Json::serialize(&data).int_err()?,
        };

        Ok(data)
    }

    pub fn get(
        &self,
        path: &str,
        scope: ConfigScope,
        with_defaults: bool,
        output_format: ConfigObjectFormat,
    ) -> Result<Option<String>, CLIError> {
        let paths = self.read_paths_for_scope(scope);
        let fig = self.get_config_object(&paths);

        let Some(value) = fig
            .get_value(path, with_defaults)
            .map_err(CLIError::usage_error_from)?
        else {
            return Ok(None);
        };

        let value = match output_format {
            ConfigObjectFormat::Yaml => setty::format::Yaml::serialize(&value).int_err()?,
            ConfigObjectFormat::Json => setty::format::Json::serialize(&value).int_err()?,
        };

        Ok(Some(value))
    }

    pub fn set(
        &self,
        path: &str,
        value: &str,
        input_format: ConfigObjectFormat,
        scope: ConfigScope,
    ) -> Result<(), CLIError> {
        let write_path = self.write_path_for_scope(scope)?;
        let read_paths = self.read_paths_for_scope(scope);

        let fig = self.get_config_object(&read_paths);

        // Parse value as YAML - this allows both unqoted strings and JSON
        let value: setty::Value = match input_format {
            ConfigObjectFormat::Yaml => {
                setty::format::Yaml::deserialize(value).map_err(CLIError::usage_error_from)?
            }
            ConfigObjectFormat::Json => {
                setty::format::Json::deserialize(value).map_err(CLIError::usage_error_from)?
            }
        };

        // Set with validation
        fig.set_value::<WithManifest<setty::format::Yaml>>(path, value, &write_path)
            .map_err(CLIError::usage_error_from)?;

        Ok(())
    }

    pub fn unset(&self, path: &str, scope: ConfigScope) -> Result<(), CLIError> {
        let write_path = self.write_path_for_scope(scope)?;
        let read_paths = self.read_paths_for_scope(scope);

        if !write_path.exists() {
            return Err(CLIError::usage_error(format!("Path {path} not found")));
        }

        let fig = self.get_config_object(&read_paths);
        if fig
            .unset_value::<WithManifest<setty::format::Yaml>>(path, &write_path)
            .map_err(CLIError::usage_error_from)?
            .is_none()
        {
            return Err(CLIError::usage_error(format!("Path {path} not found")));
        }

        Ok(())
    }

    /// Given a prefix like `some.va` would return possible completions, e.g.
    /// `some.value` and `some.validator`
    pub fn complete_path(&self, prefix: &str) -> Vec<String> {
        let fig = self.get_config_object(&[]);
        fig.complete_path(prefix)
    }

    fn read_paths_for_scope(&self, scope: ConfigScope) -> Vec<PathBuf> {
        // If config specified explicitly - use it
        if let Ok(path) = std::env::var(CONFIG_PATH_ENV_VAR) {
            return path.split(',').map(Into::into).collect();
        }

        let mut ret = Vec::new();

        match scope {
            ConfigScope::User => {
                if let Some(p) = dirs::config_local_dir() {
                    ret.push(p.join("kamu/config.yaml"));
                }
                if let Some(p) = dirs::home_dir() {
                    ret.push(p.join(CONFIG_FILENAME));
                }
            }
            ConfigScope::Workspace => {
                ret.push(self.workspace_kamu_dir.join(CONFIG_FILENAME));
            }
            ConfigScope::Combined => {
                ret.push(self.workspace_kamu_dir.join(CONFIG_FILENAME));

                let mut current = self.workspace_kamu_dir.parent();
                while let Some(cur) = current {
                    ret.push(cur.join(CONFIG_FILENAME));
                    current = cur.parent();
                }
            }
        }

        ret.retain(|p| p.is_file());
        ret.reverse();

        ret
    }

    fn write_path_for_scope(&self, scope: ConfigScope) -> Result<PathBuf, CLIError> {
        // If config specified explicitly - use it
        if let Ok(path) = std::env::var(CONFIG_PATH_ENV_VAR) {
            return Ok(path.split(',').map(Into::into).next().unwrap());
        }

        match scope {
            ConfigScope::Workspace | ConfigScope::Combined => {
                if !self.workspace_kamu_dir.exists() {
                    return Err(CLIError::usage_error_from(NotInWorkspace));
                }
                Ok(self.workspace_kamu_dir.join(CONFIG_FILENAME))
            }
            ConfigScope::User => {
                let p = dirs::config_local_dir()
                    .map(|p| p.join("kamu/config.yaml"))
                    .or_else(|| dirs::home_dir().map(|p| p.join(CONFIG_FILENAME)))
                    .expect("Cannot determine user config path");
                Ok(p)
            }
        }
    }

    fn get_config_object(&self, paths: &[PathBuf]) -> setty::Config<CLIConfig> {
        use setty::format::*;
        use setty::source::*;

        setty::Config::new()
            .with_sources(paths.iter().map(File::<WithManifest<Yaml>>::new))
            .with_source(Env::<Yaml>::new(
                CONFIG_PREFIX_ENV_VAR,
                CONFIG_SEPARATOR_ENV_VAR,
            ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// This [`setty::format::Format`] implementation is used to wrap config objects
/// with our standard [`Manifest`] for upgradeability
struct WithManifest<Fmt> {
    _p: std::marker::PhantomData<Fmt>,
}

impl<Fmt> setty::format::Format for WithManifest<Fmt>
where
    Fmt: setty::format::Format,
{
    type ErrorDe = Fmt::ErrorDe;
    type ErrorSer = Fmt::ErrorSer;

    fn name() -> std::borrow::Cow<'static, str> {
        format!("{}+manifest", Fmt::name()).into()
    }

    fn deserialize<T: serde::de::DeserializeOwned>(string: &str) -> Result<T, Self::ErrorDe> {
        let manifest: Manifest<T> = Fmt::deserialize(string)?;

        // TODO: Migrations
        assert_eq!(manifest.kind, "CLIConfig");
        assert_eq!(manifest.version, CONFIG_VERSION);

        Ok(manifest.content)
    }

    fn serialize<T: serde::ser::Serialize>(value: &T) -> Result<String, Self::ErrorSer> {
        let manifest = Manifest {
            kind: "CLIConfig".to_owned(),
            version: CONFIG_VERSION,
            content: value,
        };

        Fmt::serialize(&manifest)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
