// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::auth::{DatasetCredentials, ResolveDatasetCredentialsError};
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::{AccountName, DatasetAlias};
use url::Url;

use crate::{
    RemoteServerAccountCredentials,
    RemoteServerCredentials,
    RemoteServerCredentialsScope,
    WorkspaceLayout,
};

////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteServerCredentialsService {
    storage: Arc<dyn RemoteServerCredentialsStorage>,
    workspace_credentials: Mutex<Vec<RemoteServerCredentials>>,
    user_credentials: Mutex<Vec<RemoteServerCredentials>>,
}

#[component(pub)]
impl RemoteServerCredentialsService {
    pub fn new(storage: Arc<dyn RemoteServerCredentialsStorage>) -> Self {
        let user_credentials = storage
            .read_credentials(RemoteServerCredentialsScope::User)
            .unwrap();

        let workspace_credentials = storage
            .read_credentials(RemoteServerCredentialsScope::Workspace)
            .unwrap();

        Self {
            storage,
            user_credentials: Mutex::new(user_credentials),
            workspace_credentials: Mutex::new(workspace_credentials),
        }
    }

    pub fn find_by_frontend_url(
        &self,
        scope: RemoteServerCredentialsScope,
        remote_server_frontend_url: &Url,
        account_name: &AccountName,
    ) -> Option<RemoteServerCredentialsFindDetails> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");

        if let Some(server_credentials) = credentials_table
            .iter()
            .find(|c| &c.server_frontend_url == remote_server_frontend_url)
        {
            server_credentials.for_account(account_name).map(|ac| {
                RemoteServerCredentialsFindDetails {
                    server_backend_url: server_credentials.server_backend_url.clone(),
                    server_frontend_url: server_credentials.server_frontend_url.clone(),
                    account_credentials: ac.clone(),
                }
            })
        } else {
            None
        }
    }

    pub fn find_by_backend_url(
        &self,
        scope: RemoteServerCredentialsScope,
        remote_server_backend_url: &Url,
        account_name: &AccountName,
    ) -> Option<RemoteServerCredentialsFindDetails> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");

        if let Some(server_credentials) = credentials_table
            .iter()
            .find(|c| &c.server_backend_url == remote_server_backend_url)
        {
            server_credentials.for_account(account_name).map(|ac| {
                RemoteServerCredentialsFindDetails {
                    server_backend_url: server_credentials.server_backend_url.clone(),
                    server_frontend_url: server_credentials.server_frontend_url.clone(),
                    account_credentials: ac.clone(),
                }
            })
        } else {
            None
        }
    }

    pub fn save_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        remote_server_frontend_url: &Url,
        remote_server_backend_url: &Url,
        account_name: &AccountName,
        account_credentials: RemoteServerAccountCredentials,
    ) -> Result<(), InternalError> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let mut credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");

        if let Some(server_credentials) = credentials_table
            .iter_mut()
            .find(|c| &c.server_frontend_url == remote_server_frontend_url)
        {
            server_credentials.add_account_credentials(account_name.clone(), account_credentials);
        } else {
            let mut server_credentials = RemoteServerCredentials::new(
                remote_server_frontend_url.clone(),
                remote_server_backend_url.clone(),
            );
            server_credentials
                .add_account_credentials(account_name.clone(), account_credentials.clone());

            credentials_table.push(server_credentials);
        }

        self.storage.write_credentials(scope, &credentials_table)
    }

    pub fn drop_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        remote_server_frontend_url: &Url,
        account_name: &AccountName,
    ) -> Result<(), InternalError> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let mut credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");
        if let Some(server_credentials) = credentials_table
            .iter_mut()
            .find(|c| &c.server_frontend_url == remote_server_frontend_url)
        {
            if let Some(_) = server_credentials.drop_account_credentials(account_name) {
                return self.storage.write_credentials(scope, &credentials_table);
            }
        }

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl kamu::domain::auth::DatasetCredentialsResolver for RemoteServerCredentialsService {
    async fn resolve_dataset_credentials(
        &self,
        _dataset_alias: &DatasetAlias,
    ) -> Result<DatasetCredentials, ResolveDatasetCredentialsError> {
        unimplemented!()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

pub trait RemoteServerCredentialsStorage: Send + Sync {
    fn read_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
    ) -> Result<Vec<RemoteServerCredentials>, InternalError>;

    fn write_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        credentials: &Vec<RemoteServerCredentials>,
    ) -> Result<(), InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////

const KAMU_CREDENTIALS: &str = ".kamucredentials";
const KAMU_CREDENTIALS_VERSION: i32 = 1;

pub struct CLIRemoteServerCredentialsStorage {
    user_credentials_path: PathBuf,
    workspace_credentials_path: PathBuf,
}

#[component(pub)]
impl CLIRemoteServerCredentialsStorage {
    pub fn new(workspace_layout: &WorkspaceLayout) -> Self {
        let user_credentials_path = dirs::home_dir()
            .expect("Cannot determine user home directory")
            .join(KAMU_CREDENTIALS);

        let workspace_credentials_path: PathBuf = workspace_layout.root_dir.join(KAMU_CREDENTIALS);

        Self {
            user_credentials_path,
            workspace_credentials_path,
        }
    }

    fn credentials_path_for_scope(&self, scope: RemoteServerCredentialsScope) -> &PathBuf {
        match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials_path,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials_path,
        }
    }
}

impl RemoteServerCredentialsStorage for CLIRemoteServerCredentialsStorage {
    fn read_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
    ) -> Result<Vec<RemoteServerCredentials>, InternalError> {
        let credentials_path = self.credentials_path_for_scope(scope);

        if !credentials_path.exists() {
            return Ok(vec![]);
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(credentials_path)
            .int_err()?;

        let manifest: Manifest<Vec<RemoteServerCredentials>> =
            serde_yaml::from_reader(file).int_err()?;

        assert_eq!(manifest.kind, "RemoteServerCredentials");
        assert_eq!(manifest.version, KAMU_CREDENTIALS_VERSION);
        Ok(manifest.content)
    }

    fn write_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        credentials: &Vec<RemoteServerCredentials>,
    ) -> Result<(), InternalError> {
        let credentials_path = self.credentials_path_for_scope(scope);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(credentials_path)
            .unwrap();

        let manifest = Manifest {
            kind: "RemoteServerCredentials".to_owned(),
            version: KAMU_CREDENTIALS_VERSION,
            content: credentials,
        };

        serde_yaml::to_writer(file, &manifest).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub struct RemoteServerCredentialsFindDetails {
    pub server_frontend_url: Url,
    pub server_backend_url: Url,
    pub account_credentials: RemoteServerAccountCredentials,
}

////////////////////////////////////////////////////////////////////////////////////////
