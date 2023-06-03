// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::sync::Arc;

use dill::*;
use kamu_domain::*;
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::*;
use url::Url;

use super::*;

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
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepoName> + 's> {
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

    fn get_repository(&self, repo_name: &RepoName) -> Result<RepositoryAccessInfo, GetRepoError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(RepositoryNotFoundError {
                repo_name: repo_name.clone(),
            }
            .into());
        }

        let file = std::fs::File::open(&file_path).int_err()?;
        let manifest: Manifest<RepositoryAccessInfo> = serde_yaml::from_reader(&file).int_err()?;
        assert_eq!(manifest.kind, "Repository");
        Ok(manifest.content)
    }

    fn add_repository(&self, repo_name: &RepoName, mut url: Url) -> Result<(), AddRepoError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if file_path.exists() {
            return Err(RepositoryAlreadyExistsError {
                repo_name: repo_name.clone(),
            }
            .into());
        }

        // Ensure has trailing slash to properly handle relative links
        if !url.path().ends_with('/') {
            url.set_path(&format!("{}/", url.path()));
        }

        let manifest = Manifest {
            kind: "Repository".to_owned(),
            version: 1,
            content: RepositoryAccessInfo { url },
        };

        let file = std::fs::File::create(&file_path).int_err()?;
        serde_yaml::to_writer(file, &manifest).int_err()?;
        Ok(())
    }

    fn delete_repository(&self, repo_name: &RepoName) -> Result<(), DeleteRepoError> {
        let file_path = self.workspace_layout.repos_dir.join(repo_name);

        if !file_path.exists() {
            return Err(RepositoryNotFoundError {
                repo_name: repo_name.clone(),
            }
            .into());
        }

        std::fs::remove_file(&file_path).int_err()?;
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteRepositoryRegistryNull;

impl RemoteRepositoryRegistry for RemoteRepositoryRegistryNull {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = RepoName> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(&self, repo_name: &RepoName) -> Result<RepositoryAccessInfo, GetRepoError> {
        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }

    fn add_repository(&self, _repo_name: &RepoName, _url: Url) -> Result<(), AddRepoError> {
        Err("null registry".int_err().into())
    }

    fn delete_repository(&self, repo_name: &RepoName) -> Result<(), DeleteRepoError> {
        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }
}
