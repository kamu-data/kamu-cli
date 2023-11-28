// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use super::*;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct RemoteAliasesRegistryImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn RemoteAliasesRegistry)]
impl RemoteAliasesRegistryImpl {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }

    async fn read_config(dataset: Arc<dyn Dataset>) -> Result<DatasetConfig, InternalError> {
        match dataset.as_info_repo().get("config").await {
            Ok(bytes) => {
                let manifest: Manifest<DatasetConfig> =
                    serde_yaml::from_slice(&bytes[..]).int_err()?;
                assert_eq!(manifest.kind, "DatasetConfig");
                Ok(manifest.content)
            }
            Err(GetNamedError::Internal(e)) => Err(e),
            Err(GetNamedError::Access(e)) => Err(e.int_err()),
            Err(GetNamedError::NotFound(_)) => Ok(DatasetConfig::default()),
        }
    }

    async fn write_config(
        dataset: Arc<dyn Dataset>,
        config: &DatasetConfig,
    ) -> Result<(), InternalError> {
        let manifest = Manifest {
            kind: "DatasetConfig".to_owned(),
            version: 1,
            content: config.clone(),
        };
        let manifest_yaml = serde_yaml::to_string(&manifest).int_err()?;
        dataset
            .as_info_repo()
            .set("config", manifest_yaml.as_bytes())
            .await
            .int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RemoteAliasesRegistry for RemoteAliasesRegistryImpl {
    async fn get_remote_aliases(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError> {
        let dataset = self.dataset_repo.get_dataset(dataset_ref).await?;
        let config = Self::read_config(dataset.clone()).await?;
        Ok(Box::new(RemoteAliasesImpl::new(dataset, config)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasesImpl
////////////////////////////////////////////////////////////////////////////////////////

struct RemoteAliasesImpl {
    dataset: Arc<dyn Dataset>,
    config: DatasetConfig,
}

impl RemoteAliasesImpl {
    fn new(dataset: Arc<dyn Dataset>, config: DatasetConfig) -> Self {
        Self { dataset, config }
    }
}

#[async_trait::async_trait]
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

    async fn add(
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
            RemoteAliasesRegistryImpl::write_config(self.dataset.clone(), &self.config).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete(
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
            RemoteAliasesRegistryImpl::write_config(self.dataset.clone(), &self.config).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn clear(&mut self, kind: RemoteAliasKind) -> Result<usize, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };
        let len = aliases.len();
        if !aliases.is_empty() {
            aliases.clear();
            RemoteAliasesRegistryImpl::write_config(self.dataset.clone(), &self.config).await?;
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

////////////////////////////////////////////////////////////////////////////////////////
