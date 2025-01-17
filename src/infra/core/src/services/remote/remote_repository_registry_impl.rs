// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::convert::TryInto;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use internal_error::{ErrorIntoInternal, ResultIntoInternal};
use kamu_core::*;
use odf::metadata::serde::yaml::Manifest;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct RemoteRepositoryRegistryImpl {
    repos_dir: Arc<RemoteReposDir>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn RemoteRepositoryRegistry)]
impl RemoteRepositoryRegistryImpl {
    pub fn new(repos_dir: Arc<RemoteReposDir>) -> Self {
        Self { repos_dir }
    }

    pub fn create(repos_dir: impl Into<PathBuf>) -> Result<Self, std::io::Error> {
        let repos_dir = Arc::new(RemoteReposDir::new(repos_dir));
        std::fs::create_dir_all(repos_dir.as_path())?;
        Ok(Self::new(repos_dir))
    }

    pub fn get_repository_file_path(&self, repo_name: &odf::RepoName) -> Option<PathBuf> {
        let file_path = self.repos_dir.join(repo_name);

        if !file_path.exists() {
            // run full scan to support case-insensitive matches
            let all_repositories_stream = self.get_all_repositories();
            for repository_name in all_repositories_stream {
                if &repository_name == repo_name {
                    return Some(self.repos_dir.join(repository_name));
                }
            }
            return None;
        }

        Some(file_path)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl RemoteRepositoryRegistry for RemoteRepositoryRegistryImpl {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = odf::RepoName> + 's> {
        let read_dir = std::fs::read_dir(self.repos_dir.as_path()).unwrap();
        Box::new(read_dir.map(|i| {
            i.unwrap()
                .file_name()
                .into_string()
                .unwrap()
                .try_into()
                .unwrap()
        }))
    }

    fn get_repository(
        &self,
        repo_name: &odf::RepoName,
    ) -> Result<RepositoryAccessInfo, GetRepoError> {
        if let Some(file_path) = self.get_repository_file_path(repo_name) {
            let file = std::fs::File::open(file_path).int_err()?;
            let manifest: Manifest<RepositoryAccessInfo> =
                serde_yaml::from_reader(&file).int_err()?;
            assert_eq!(manifest.kind, "Repository");
            return Ok(manifest.content);
        }

        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }

    fn add_repository(&self, repo_name: &odf::RepoName, mut url: Url) -> Result<(), AddRepoError> {
        if self.get_repository_file_path(repo_name).is_some() {
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

        let file = std::fs::File::create(self.repos_dir.join(repo_name)).int_err()?;
        serde_yaml::to_writer(file, &manifest).int_err()?;
        Ok(())
    }

    fn delete_repository(&self, repo_name: &odf::RepoName) -> Result<(), DeleteRepoError> {
        if let Some(file_path) = self.get_repository_file_path(repo_name) {
            std::fs::remove_file(file_path).int_err()?;
            return Ok(());
        }
        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Null
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteRepositoryRegistryNull;

impl RemoteRepositoryRegistry for RemoteRepositoryRegistryNull {
    fn get_all_repositories<'s>(&'s self) -> Box<dyn Iterator<Item = odf::RepoName> + 's> {
        Box::new(std::iter::empty())
    }

    fn get_repository(
        &self,
        repo_name: &odf::RepoName,
    ) -> Result<RepositoryAccessInfo, GetRepoError> {
        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }

    fn add_repository(&self, _repo_name: &odf::RepoName, _url: Url) -> Result<(), AddRepoError> {
        Err("null registry".int_err().into())
    }

    fn delete_repository(&self, repo_name: &odf::RepoName) -> Result<(), DeleteRepoError> {
        Err(RepositoryNotFoundError {
            repo_name: repo_name.clone(),
        }
        .into())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Config
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteReposDir(PathBuf);

impl RemoteReposDir {
    pub fn new(inner: impl Into<PathBuf>) -> Self {
        Self(inner.into())
    }
    pub fn inner(&self) -> &PathBuf {
        &self.0
    }

    pub fn into_inner(self) -> PathBuf {
        self.0
    }
}

impl AsRef<Path> for RemoteReposDir {
    fn as_ref(&self) -> &Path {
        self.0.as_path()
    }
}

impl Deref for RemoteReposDir {
    type Target = PathBuf;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
