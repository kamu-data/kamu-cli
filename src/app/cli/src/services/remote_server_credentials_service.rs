// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use dill::component;
use internal_error::{InternalError, ResultIntoInternal};
use kamu::domain::auth::{DatasetCredentials, ResolveDatasetCredentialsError};
use opendatafabric::serde::yaml::Manifest;
use opendatafabric::DatasetAlias;
use url::Url;

use crate::{
    RemoteServerCredentials,
    RemoteServerCredentialsScope,
    WorkspaceLayout,
    DEFAULT_LOGIN_URL,
};

////////////////////////////////////////////////////////////////////////////////////////

pub struct RemoteServerCredentialsService {
    storage: Arc<dyn RemoteServerCredentialsStorage>,
    workspace_credentials: Mutex<HashMap<Url, RemoteServerCredentials>>,
    user_credentials: Mutex<HashMap<Url, RemoteServerCredentials>>,
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

    fn resolve_server_url(server: Option<Url>) -> Url {
        if let Some(server_url) = server {
            server_url
        } else {
            Url::parse(DEFAULT_LOGIN_URL).unwrap()
        }
    }

    pub fn find_existing_credentials(
        &self,
        scope: Option<RemoteServerCredentialsScope>,
        server: Option<Url>,
    ) -> Option<RemoteServerCredentials> {
        let server_url = Self::resolve_server_url(server);

        match scope {
            Some(RemoteServerCredentialsScope::User) => self
                .user_credentials
                .lock()
                .expect("Could not lock credentials table")
                .get(&server_url)
                .map(|c| c.clone()),

            Some(RemoteServerCredentialsScope::Workspace) => self
                .workspace_credentials
                .lock()
                .expect("Could not lock credentials table")
                .get(&server_url)
                .map(|c| c.clone()),

            None => {
                if let Some(credentials) = self
                    .workspace_credentials
                    .lock()
                    .expect("Could not lock credentials table")
                    .get(&server_url)
                {
                    Some(credentials.clone())
                } else {
                    self.user_credentials
                        .lock()
                        .expect("Could not lock credentials table")
                        .get(&server_url)
                        .map(|c| c.clone())
                }
            }
        }
    }

    pub fn save_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        credentials: RemoteServerCredentials,
    ) -> Result<(), InternalError> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let mut credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");

        credentials_table.insert(credentials.server.clone(), credentials);

        self.storage.write_credentials(scope, &credentials_table)
    }

    pub fn drop_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        server: Option<Url>,
    ) -> Result<(), InternalError> {
        let credentials_table_ptr = match scope {
            RemoteServerCredentialsScope::User => &self.user_credentials,
            RemoteServerCredentialsScope::Workspace => &self.workspace_credentials,
        };

        let mut credentials_table = credentials_table_ptr
            .lock()
            .expect("Could not lock credentials table");

        let server_url = Self::resolve_server_url(server);

        credentials_table.remove(&server_url);

        self.storage.write_credentials(scope, &credentials_table)
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
    ) -> Result<HashMap<Url, RemoteServerCredentials>, InternalError>;

    fn write_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        credentials: &HashMap<Url, RemoteServerCredentials>,
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
    ) -> Result<HashMap<Url, RemoteServerCredentials>, InternalError> {
        let credentials_path = self.credentials_path_for_scope(scope);

        if !credentials_path.exists() {
            return Ok(HashMap::new());
        }

        let file = std::fs::OpenOptions::new()
            .read(true)
            .open(credentials_path)
            .int_err()?;

        let manifest: Manifest<Vec<RemoteServerCredentials>> =
            serde_yaml::from_reader(file).int_err()?;

        assert_eq!(manifest.kind, "RemoteServerCredentials");
        assert_eq!(manifest.version, KAMU_CREDENTIALS_VERSION);

        let mut res = HashMap::new();

        for credentials in manifest.content {
            res.insert(credentials.server.clone(), credentials);
        }

        Ok(res)
    }

    fn write_credentials(
        &self,
        scope: RemoteServerCredentialsScope,
        credentials: &HashMap<Url, RemoteServerCredentials>,
    ) -> Result<(), InternalError> {
        let credentials_path = self.credentials_path_for_scope(scope);

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(credentials_path)
            .unwrap();

        let mut content: Vec<&RemoteServerCredentials> = Vec::new();
        for credentials in credentials.values() {
            content.push(credentials);
        }

        let manifest = Manifest {
            kind: "RemoteServerCredentials".to_owned(),
            version: KAMU_CREDENTIALS_VERSION,
            content,
        };

        serde_yaml::to_writer(file, &manifest).int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////
