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
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_core::*;
use odf::metadata::serde::yaml::Manifest;
use thiserror::Error;

use crate::DatasetConfig;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn RemoteAliasesRegistry)]
pub struct RemoteAliasesRegistryImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteAliasesRegistryImpl {
    async fn read_config(dataset: &dyn odf::Dataset) -> Result<DatasetConfig, InternalError> {
        match dataset.as_info_repo().get("config").await {
            Ok(bytes) => {
                let manifest: Manifest<DatasetConfig> =
                    serde_yaml::from_slice(&bytes[..]).int_err()?;
                assert_eq!(manifest.kind, "DatasetConfig");
                Ok(manifest.content)
            }
            Err(odf::storage::GetNamedError::Internal(e)) => Err(e),
            Err(odf::storage::GetNamedError::Access(e)) => Err(e.int_err()),
            Err(odf::storage::GetNamedError::NotFound(_)) => Ok(DatasetConfig::default()),
        }
    }

    async fn write_config(
        dataset: &dyn odf::Dataset,
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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl RemoteAliasesRegistry for RemoteAliasesRegistryImpl {
    #[tracing::instrument(level = "debug", skip_all, fields(?dataset_handle))]
    async fn get_remote_aliases(
        &self,
        dataset_handle: &odf::DatasetHandle,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError> {
        let resolved_dataset = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        let config = Self::read_config(resolved_dataset.as_ref()).await?;
        tracing::debug!(?config, "Loaded dataset config");

        Ok(Box::new(RemoteAliasesImpl::new(resolved_dataset, config)))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// RemoteAliasesImpl
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct RemoteAliasesImpl {
    resolved_dataset: ResolvedDataset,
    config: DatasetConfig,
}

impl RemoteAliasesImpl {
    fn new(resolved_dataset: ResolvedDataset, config: DatasetConfig) -> Self {
        Self {
            resolved_dataset,
            config,
        }
    }
}

#[async_trait::async_trait]
impl RemoteAliases for RemoteAliasesImpl {
    fn get_by_kind<'a>(
        &'a self,
        kind: RemoteAliasKind,
    ) -> Box<dyn Iterator<Item = &'a odf::DatasetRefRemote> + 'a> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &self.config.pull_aliases,
            RemoteAliasKind::Push => &self.config.push_aliases,
        };
        Box::new(aliases.iter())
    }

    fn contains(&self, remote_ref: &odf::DatasetRefRemote, kind: RemoteAliasKind) -> bool {
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
        remote_ref: &odf::DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        let remote_ref = remote_ref.to_owned();
        if !aliases.contains(&remote_ref) {
            aliases.push(remote_ref);
            RemoteAliasesRegistryImpl::write_config(self.resolved_dataset.as_ref(), &self.config)
                .await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn delete(
        &mut self,
        remote_ref: &odf::DatasetRefRemote,
        kind: RemoteAliasKind,
    ) -> Result<bool, InternalError> {
        let aliases = match kind {
            RemoteAliasKind::Pull => &mut self.config.pull_aliases,
            RemoteAliasKind::Push => &mut self.config.push_aliases,
        };

        if let Some(i) = aliases.iter().position(|r| *r == *remote_ref) {
            aliases.remove(i);
            RemoteAliasesRegistryImpl::write_config(self.resolved_dataset.as_ref(), &self.config)
                .await?;
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
            RemoteAliasesRegistryImpl::write_config(self.resolved_dataset.as_ref(), &self.config)
                .await?;
        }
        Ok(len)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteAliasesRegistryNull;

#[async_trait::async_trait]
impl RemoteAliasesRegistry for RemoteAliasesRegistryNull {
    async fn get_remote_aliases(
        &self,
        _dataset_handle: &odf::DatasetHandle,
    ) -> Result<Box<dyn RemoteAliases>, GetAliasesError> {
        #[derive(Error, Debug)]
        #[error("get_remote_aliases requested from stub implementation")]
        struct NullError {}

        let e = NullError {};
        Err(GetAliasesError::Internal(e.int_err()))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
