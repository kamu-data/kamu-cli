// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use crate::domain::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;

use dill::*;
use std::convert::TryInto;
use std::sync::Arc;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct RemoteRepositoryRegistryImpl {
    workspace_layout: Arc<WorkspaceLayout>,
}

////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
impl RemoteRepositoryRegistryImpl {
    pub fn new(workspace_layout: Arc<WorkspaceLayout>) -> Self {
        Self { workspace_layout }
    }
}

////////////////////////////////////////////////////////////////////////////////////////

impl RemoteRepositoryRegistry for RemoteRepositoryRegistryImpl {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's> {
        let read_dir = std::fs::read_dir(&self.workspace_layout.repos_dir).unwrap();
        Box::new(read_dir.map(|i| {
            i.unwrap()
                .file_name()
                .into_string()
                .unwrap()
                .try_into()
                .unwrap()
        }))
    }

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        let file = std::fs::File::open(&file_path).unwrap_or_else(|e| {
            panic!(
                "Failed to open the Repository file at {}: {}",
                file_path.display(),
                e
            )
        });

        let manifest: Manifest<Repository> = serde_yaml::from_reader(&file).unwrap_or_else(|e| {
            panic!(
                "Failed to deserialize the Repository at {}: {}",
                file_path.display(),
                e
            )
        });

        assert_eq!(manifest.kind, "Repository");
        Ok(manifest.content)
    }

    fn add_repository(&self, repo_name: &RepositoryName, url: Url) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if file_path.exists() {
            return Err(DomainError::already_exists(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        let manifest = Manifest {
            kind: "Repository".to_owned(),
            version: 1,
            content: Repository { url: url },
        };

        let file = std::fs::File::create(&file_path).map_err(|e| InfraError::from(e).into())?;
        serde_yaml::to_writer(file, &manifest).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(DomainError::does_not_exist(
                ResourceKind::Repository,
                repo_name.to_string(),
            ));
        }

        std::fs::remove_file(&file_path).map_err(|e| InfraError::from(e).into())?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteRepositoryRegistryNull;

impl RemoteRepositoryRegistry for RemoteRepositoryRegistryNull {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepositoryName> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(&self, repo_name: &RepositoryName) -> Result<Repository, DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_name.to_string(),
        ))
    }

    fn add_repository(&self, _repo_name: &RepositoryName, _url: Url) -> Result<(), DomainError> {
        Err(DomainError::ReadOnly)
    }

    fn delete_repository(&self, repo_name: &RepositoryName) -> Result<(), DomainError> {
        Err(DomainError::does_not_exist(
            ResourceKind::Repository,
            repo_name.to_string(),
        ))
    }
}
