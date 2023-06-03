// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use dill::*;
use kamu_domain::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct RemoteAliasesRegistryImpl {
    local_repo: Arc<dyn DatasetRepository>,
    workspace_layout: Arc<WorkspaceLayout>,
}

////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl RemoteAliasesRegistryImpl {
    pub fn new(
        local_repo: Arc<dyn DatasetRepository>,
        workspace_layout: Arc<WorkspaceLayout>,
    ) -> Self {
        Self {
            local_repo,
            workspace_layout,
        }
    }

    fn get_dataset_metadata_dir(&self, alias: &DatasetAlias) -> PathBuf {
        assert!(!alias.is_multitenant(), "Multitenancy is not supported");
        self.workspace_layout.datasets_dir.join(&alias.dataset_name)
    }

    fn read_config(&self, path: &Path) -> Result<DatasetConfig, InternalError> {
        let file = std::fs::File::open(&path).int_err()?;

        let manifest: Manifest<DatasetConfig> = serde_yaml::from_reader(&file).int_err()?;

        assert_eq!(manifest.kind, "DatasetConfig");
        Ok(manifest.content)
    }

    fn write_config(&self, path: &Path, config: DatasetConfig) -> Result<(), InternalError> {
        let manifest = Manifest {
            kind: "DatasetConfig".to_owned(),
            version: 1,
            content: config,
        };
        let file = std::fs::File::create(&path).int_err()?;
        serde_yaml::to_writer(file, &manifest).int_err()?;
        Ok(())
    }

    fn get_config(&self, dataset_alias: &DatasetAlias) -> Result<DatasetConfig, InternalError> {
        let path = self.get_dataset_metadata_dir(dataset_alias).join("config");

        if path.exists() {
            self.read_config(&path)
        } else {
            Ok(DatasetConfig::default())
        }
    }

    fn set_config(
        &self,
        dataset_alias: &DatasetAlias,
        config: DatasetConfig,
    ) -> Result<(), InternalError> {
        let path = self.get_dataset_metadata_dir(dataset_alias).join("config");
        self.write_config(&path, config)
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RemoteAliasesRegistry for RemoteAliasesRegistryImpl {
    async fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError> {
        let hdl = self.local_repo.resolve_dataset_ref(dataset_ref).await?;
        let config = self.get_config(&hdl.alias)?;
        Ok(Box::new(RemoteAliasesImpl::new(self.clone(), hdl, config)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasesImpl
////////////////////////////////////////////////////////////////////////////////////////

struct RemoteAliasesImpl {
    alias_registry: RemoteAliasesRegistryImpl,
    dataset_handle: DatasetHandle,
    config: DatasetConfig,
}

impl RemoteAliasesImpl {
    fn new(
        alias_registry: RemoteAliasesRegistryImpl,
        dataset_handle: DatasetHandle,
        config: DatasetConfig,
    ) -> Self {
        Self {
            alias_registry,
            dataset_handle,
            config,
        }
    }
}

impl RemoteAliases for RemoteAliasesImpl {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a DatasetRefRemote> + 'a> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        Box::new(aliases.iter())
    }

    fn contains(&self, remote_ref: &DatasetRefRemote, kind: RemoteAliasKind) -> bool {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        for a in aliases {
            if *a == *remote_ref {
                return true;
            }
        }
        false
    }

    fn is_empty(&self, kind: RemoteAliasKind) -> bool {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        aliases.is_empty()
    }

    fn add(
        &mut self,
        remote_ref: &DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        let remote_ref = remote_ref.to_owned();
        if !aliases.contains(&remote_ref) {
            aliases.push(remote_ref);
            self.alias_registry
                .set_config(&self.dataset_handle.alias, self.config.clone())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn delete(
        &mut self,
        remote_ref: &DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        if let Some(i) = aliases.iter().position(|r| *r == *remote_ref) {
            aliases.remove(i);
            self.alias_registry
                .set_config(&self.dataset_handle.alias, self.config.clone())?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };
        let len = aliases.len();
        if !aliases.is_empty() {
            aliases.clear();
            self.alias_registry
                .set_config(&self.dataset_handle.alias, self.config.clone())?;
        }
        Ok(len)
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasesRegistryNull;

#[async_trait::async_trait]
impl RemoteAliasesRegistry for RemoteAliasesRegistryNull {
    async fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError> {
        Err(DatasetNotFoundError {
            dataset_ref: dataset_ref.clone(),
        }
        .into())
    }
}
